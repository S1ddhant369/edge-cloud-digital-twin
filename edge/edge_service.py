import json, os, sqlite3, threading, time, uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import paho.mqtt.client as mqtt

DB_PATH = os.getenv("DB_PATH", "/data/edge.db")
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "twin-events")
MAX_ROWS = int(os.getenv("BUFFER_MAX_ROWS", "500"))
SYNC_INTERVAL = float(os.getenv("SYNC_INTERVAL_SECONDS", "1"))
lock = threading.RLock()
network_online = True
producer = None
stats = {"received": 0, "evicted": 0, "sent_to_kafka": 0, "sync_errors": 0}

def connect_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(
    DB_PATH,
    timeout=10,
    check_same_thread=False
)
    con.row_factory = sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS events(
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE NOT NULL,
        twin_id TEXT NOT NULL,
        source TEXT NOT NULL,
        value REAL NOT NULL,
        event_ts REAL NOT NULL,
        created_ts REAL NOT NULL,
        synced INTEGER NOT NULL DEFAULT 0
    )""")
    con.commit()
    return con

db = connect_db()

def now():
    return datetime.now(timezone.utc).isoformat()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        event = {
            "event_id": str(payload.get("event_id") or uuid.uuid4()),
            "twin_id": str(payload.get("twin_id", "machine-01")),
            "source": str(payload.get("source", "edge")),
            "value": float(payload["value"]),
            "event_ts": float(payload.get("event_ts", time.time())),
            "created_ts": time.time()
        }
        with lock:
            db.execute("""INSERT OR IGNORE INTO events
              (event_id,twin_id,source,value,event_ts,created_ts,synced)
              VALUES (?,?,?,?,?,?,0)""",
              (event["event_id"], event["twin_id"], event["source"], event["value"],
               event["event_ts"], event["created_ts"]))
            db.commit()
            stats["received"] += 1
            count = db.execute("SELECT COUNT(*) FROM events WHERE synced=0").fetchone()[0]
            while count > MAX_ROWS:
                old = db.execute("SELECT seq FROM events WHERE synced=0 ORDER BY seq LIMIT 1").fetchone()
                if not old: break
                db.execute("DELETE FROM events WHERE seq=?", (old["seq"],))
                db.commit()
                stats["evicted"] += 1
                count -= 1
    except Exception as exc:
        print("Invalid MQTT message:", exc, flush=True)

def connect_kafka():
    global producer
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5, acks="all", request_timeout_ms=5000,
                max_block_ms=3000
            )
            print("Connected to Kafka", flush=True)
        except Exception as exc:
            print("Kafka not ready; retrying:", exc, flush=True)
            time.sleep(3)

def sync_loop():
    connect_kafka()
    while True:
        time.sleep(SYNC_INTERVAL)
        with lock:
            if not network_online:
                continue
            rows = db.execute("SELECT * FROM events WHERE synced=0 ORDER BY seq LIMIT 100").fetchall()
        for row in rows:
            try:
                event = {k: row[k] for k in ("event_id","twin_id","source","value","event_ts","created_ts")}
                producer.send(TOPIC, event, key=event["twin_id"].encode()).get(timeout=5)
                with lock:
                    db.execute("UPDATE events SET synced=1 WHERE event_id=?", (event["event_id"],))
                    db.commit()
                    stats["sent_to_kafka"] += 1
            except Exception as exc:
                stats["sync_errors"] += 1
                print("Sync failed; event remains buffered:", exc, flush=True)
                time.sleep(1)
                break

def set_network(online):
    global network_online
    with lock:
        network_online = bool(online)

def snapshot():
    with lock:
        total = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM events WHERE synced=0").fetchone()[0]
        oldest = db.execute("SELECT MIN(created_ts) FROM events WHERE synced=0").fetchone()[0]
        return {"tier":"edge", "network_online":network_online, "buffer_max_rows":MAX_ROWS,
                "buffer_total_rows":total, "pending_rows":pending,
                "oldest_pending_age_seconds": round(time.time()-oldest, 3) if oldest else 0,
                "stats":stats.copy()}

class Handler(BaseHTTPRequestHandler):
    def respond(self, code, obj):
        body=json.dumps(obj, indent=2).encode()
        self.send_response(code); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        if self.path == "/health": self.respond(200, {"status":"ok"})
        elif self.path == "/metrics": self.respond(200, snapshot())
        else: self.respond(404, {"error":"Use /health or /metrics"})
    def do_POST(self):
        if self.path == "/network":
            try:
                body=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))))
                if "online" not in body: raise ValueError("JSON must include boolean 'online'")
                set_network(body["online"])
                self.respond(200, {"network_online":network_online})
            except Exception as exc: self.respond(400, {"error":str(exc)})
        elif self.path == "/publish":
            try:
                body=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))))
                body.setdefault("event_id", str(uuid.uuid4()))
                body.setdefault("source","edge")
                body.setdefault("event_ts",time.time())
                body.setdefault("twin_id","machine-01")
                body["value"]=float(body["value"])
                client.publish("twin/machine-01/reading", json.dumps(body), qos=1)
                self.respond(202, {"accepted":True,"event_id":body["event_id"]})
            except Exception as exc: self.respond(400, {"error":str(exc)})
        else: self.respond(404, {"error":"Use /network or /publish"})
    def log_message(self, *args): pass

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="edge-subscriber")
client.on_message = on_message
for attempt in range(30):
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60); break
    except Exception:
        time.sleep(1)
else:
    raise RuntimeError("Cannot connect to local Mosquitto")
client.subscribe("twin/+/reading", qos=1)
client.loop_start()
threading.Thread(target=sync_loop, daemon=True).start()
print("Edge service ready; API on :8080", flush=True)
ThreadingHTTPServer(("0.0.0.0", int(os.getenv("EDGE_API_PORT","8080"))), Handler).serve_forever()
