from __future__ import annotations
import time, random, itertools
_HAS_SPARK = False
from models import Player, GameMatch, CheatAlert, ServerAlert, TelemetryEvent, ServerMetricEvent
from state import GameState
from producer import Producer
SPEED_LIMIT=260; TELEPORT_DISTANCE=400; TEAM_SIZE=3; MATCH_SIZE=6; SKILL_WINDOW=18; PING_WINDOW=60; MAX_QUEUE_WAIT=8000
SERVER_CPU_SPIKE=90; SERVER_CPU_DELTA=30; SERVER_PACKET_LOSS=3.0
_alert_seq=itertools.count(1); _match_seq=itertools.count(1)
def _alert_id(): 
    return "a_" + str(next(_alert_seq)) + "_" + random.random().hex()[2:6]
def _match_id(): 
    return "m_" + str(next(_match_seq))
class Consumer:
    def __init__(self, state: GameState, producer: Producer, spark=None, server_producer=None):
        self.state=state; self.producer=producer; self.server_producer=server_producer; self.spark=spark
        self._last_pos_rows=[]; self._last_server_rows=[]; self._server_alerted=set(); self.batches_processed=0; self.last_error=None
    def reduce_batch(self, batch):
        self.batches_processed+=1
        for ev in batch:
            p=self.state.players.get(getattr(ev,'player_id'))
            if not p: continue
            if getattr(ev,'type')=='kill':
                p.kills=getattr(p,'kills',0)+1
                vid=getattr(ev,'victim_id')
                if vid and vid in self.state.players: 
                    self.state.players[vid].deaths=getattr(self.state.players[vid],'deaths',0)+1
            p.ping=getattr(ev,'ping',p.ping); p.x=getattr(ev,'x',p.x); p.y=getattr(ev,'y',p.y); p.last_event_at=getattr(ev,'timestamp',time.time()*1000)
        feed=[e for e in batch if getattr(e,'type') in ('kill','action','death')]; feed.reverse(); return feed[-40:]
    def process(self,batch):
        feed=self.reduce_batch(batch); pos=[e for e in batch if getattr(e,'type')=='position']
        if not pos: return feed,[]
        return feed,[]
    def _detect_cheats_spark(self,position_events): return []
    def reduce_server_batch(self,events):
        for ev in events:
            info=self.state.servers.get(getattr(ev,'server_id'))
            if not info: continue
            info.cpu=getattr(ev,'cpu',info.cpu); info.memory=getattr(ev,'memory',info.memory); info.tick_rate=getattr(ev,'tick_rate',info.tick_rate); info.packet_loss=getattr(ev,'packet_loss',info.packet_loss); info.active_players=getattr(ev,'active_players',info.active_players); info.last_event_at=getattr(ev,'timestamp',time.time()*1000)
            if not (getattr(info,'cpu',0)>=SERVER_CPU_SPIKE or getattr(info,'packet_loss',0)>SERVER_PACKET_LOSS): 
                self._server_alerted.discard(info.id)
    def _detect_server_anomalies_spark(self,events): return []
    def rank_queue_spark(self):
        queued=[p for p in self.state.players.values() if p.status=='in_queue']
        if len(queued)<MATCH_SIZE: return []
        from collections import defaultdict
        by_reg=defaultdict(list)
        for p in queued: by_reg[p.region].append((p,p.queue_enter_at))
        res=[]
        for reg,items in by_reg.items():
            items_sorted=sorted(items, key=lambda x:(x[0].skill,x[1]))
            class S: pass
            out=[]
            for i,(p,qt) in enumerate(items_sorted):
                s=S(); s.skill_rank=i+1; s.id=p.id; s.name=p.name; s.skill=p.skill; s.ping=p.ping; s.queue_enter_at=p.queue_enter_at; out.append(s)
            res.append((reg,out))
        return res
    def form_matches(self,ranked_regions):
        now=time.time()*1000; new_matches=[]
        for region,ranked_players in ranked_regions:
            if len(ranked_players)<MATCH_SIZE: continue
            players_in_region=list(ranked_players); i=0
            while i+MATCH_SIZE<=len(players_in_region):
                window=players_in_region[i:i+MATCH_SIZE]
                waited_long=any(now-getattr(p,'queue_enter_at',0)>MAX_QUEUE_WAIT for p in window)
                skill_spread=window[-1].skill-window[0].skill
                pings=[getattr(p,'ping',0) for p in window]; ping_spread=max(pings)-min(pings)
                skill_ok=skill_spread<=(SKILL_WINDOW*2 if waited_long else SKILL_WINDOW)
                ping_ok=ping_spread<=(PING_WINDOW*2 if waited_long else PING_WINDOW)
                if skill_ok and ping_ok:
                    team_a=[p.id for p in window[:TEAM_SIZE]]; team_b=[p.id for p in window[TEAM_SIZE:]]; all_ids=team_a+team_b
                    avg_skill=round(sum(getattr(p,'skill',0) for p in window)/len(window)); avg_ping=round(sum(getattr(p,'ping',0) for p in window)/len(window))
                    match=GameMatch(id=_match_id(), players=all_ids, team_a=team_a, team_b=team_b, avg_skill=avg_skill, avg_ping=avg_ping, region=region, created_at=now)
                    self.state.matches.append(match); self.state.matches_formed+=1
                    for r in window:
                        p=self.state.players.get(r.id)
                        if not p: continue
                        p.status='in_match'; p.match_id=match.id; p.match_enter_at=now; p.x=random.random()*1000; p.y=random.random()*1000; p.target_x=random.random()*1000; p.target_y=random.random()*1000; p.kills=0; p.deaths=0
                        try: self.producer.on_player_enter_match(p)
                        except Exception: pass
                    if self.server_producer:
                        try: self.server_producer.on_server_spawned(match)
                        except Exception: pass
                    new_matches.append(match); matched=set(all_ids); players_in_region=[p for p in players_in_region if p.id not in matched]; i=0
                else: i+=1
        return new_matches
    def run_matchmaking(self): return self.form_matches(self.rank_queue_spark())
    def on_match_ended(self,match,players):
        for p in players:
            try: self.producer.on_player_leave_match(p)
            except Exception: pass
        if self.server_producer:
            try: self.server_producer.on_server_stopped(match)
            except Exception: pass
        self._server_alerted.discard('srv_' + match.id)
    def clear_alerts(self):
        self.state.alerts=[]; self.state.flagged_players.clear(); self.state.server_alerts=[]; self._server_alerted.clear()
        for p in self.state.players.values():
            if hasattr(p,'flagged_reasons'): p.flagged_reasons=[]
