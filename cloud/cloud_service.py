import json, os, sqlite3, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from kafka import KafkaConsumer

DB_PATH=os.getenv("DB_PATH","/data/cloud.db")
TOPIC=os.getenv("KAFKA_TOPIC","twin-events")
BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
os.makedirs(os.path.dirname(DB_PATH) or ".",exist_ok=True)
db=sqlite3.connect(DB_PATH,check_same_thread=False,timeout=10)
db.row_factory=sqlite3.Row
db.execute("""CREATE TABLE IF NOT EXISTS twin_state(
 twin_id TEXT PRIMARY KEY, value REAL NOT NULL, source TEXT NOT NULL,
 event_ts REAL NOT NULL, event_id TEXT NOT NULL, received_ts REAL NOT NULL)""")
db.execute("""CREATE TABLE IF NOT EXISTS processed_events(event_id TEXT PRIMARY KEY)""")
db.commit()
counts={"received":0,"applied":0,"duplicates":0,"conflicts":0}
def consume():
    while True:
        try:
            consumer=KafkaConsumer(TOPIC,bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda b:json.loads(b.decode()),
                key_deserializer=lambda b:b.decode() if b else None,
                auto_offset_reset="earliest",enable_auto_commit=True,
                group_id="digital-twin-cloud-v1")
            print("Cloud consumer connected to Kafka",flush=True)
            for msg in consumer:
                e=msg.value; counts["received"]+=1
                cur=db.cursor()
                try:
                    cur.execute("INSERT INTO processed_events(event_id) VALUES(?)",(e["event_id"],))
                except sqlite3.IntegrityError:
                    counts["duplicates"]+=1; db.commit(); continue
                old=cur.execute("SELECT * FROM twin_state WHERE twin_id=?",(e["twin_id"],)).fetchone()
                # Deterministic conflict policy: latest event timestamp wins.
                # Equal timestamps: cloud-origin event wins; then event_id lexical order.
                incoming=(float(e["event_ts"]), 1 if e["source"]=="cloud" else 0, str(e["event_id"]))
                existing=(old["event_ts"],1 if old["source"]=="cloud" else 0,old["event_id"]) if old else None
                if existing is None or incoming > existing:
                    if old and (old["value"] != e["value"] or old["source"] != e["source"]):
                        counts["conflicts"]+=1
                    cur.execute("""INSERT INTO twin_state VALUES(?,?,?,?,?,?)
                     ON CONFLICT(twin_id) DO UPDATE SET value=excluded.value,source=excluded.source,
                     event_ts=excluded.event_ts,event_id=excluded.event_id,received_ts=excluded.received_ts""",
                     (e["twin_id"],e["value"],e["source"],e["event_ts"],e["event_id"],time.time()))
                    counts["applied"]+=1
                elif old and (old["value"] != e["value"] or old["event_ts"] == e["event_ts"]):
                    counts["conflicts"]+=1
                db.commit()
        except Exception as exc:
            print("Kafka consumer unavailable; retrying:",exc,flush=True); time.sleep(3)

class Handler(BaseHTTPRequestHandler):
    def send_json(self,obj):
        b=json.dumps(obj,indent=2).encode(); self.send_response(200)
        self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if self.path=="/health": self.send_json({"status":"ok"})
        elif self.path=="/state":
            rows=[dict(r) for r in db.execute("SELECT * FROM twin_state ORDER BY twin_id").fetchall()]
            self.send_json({"state":rows,"counts":counts})
        elif self.path=="/metrics":
            self.send_json({"tier":"cloud","counts":counts.copy(),
                "state_rows":db.execute("SELECT COUNT(*) FROM twin_state").fetchone()[0]})
        else: self.send_response(404); self.end_headers()
    def log_message(self,*args): pass

import threading
threading.Thread(target=consume,daemon=True).start()
print("Cloud API ready on :8081",flush=True)
ThreadingHTTPServer(("0.0.0.0",8081),Handler).serve_forever()
