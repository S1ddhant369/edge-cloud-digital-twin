"""Run three offline-duration trials against a running Docker Compose project.
Requires Python 3.10+ and requests. Sensor is optional; this script posts readings itself.
"""
import json, time, urllib.request, urllib.error, uuid, statistics

EDGE="http://localhost:8080"
CLOUD="http://localhost:8081"

def req(url, method="GET", data=None, timeout=5):
    raw=None if data is None else json.dumps(data).encode()
    r=urllib.request.Request(url,data=raw,method=method,headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(r,timeout=timeout) as response:
        return json.loads(response.read().decode())

def wait_for(predicate, timeout=40, interval=.25):
    end=time.time()+timeout
    while time.time()<end:
        try:
            if predicate(): return True
        except Exception: pass
        time.sleep(interval)
    return False

def trial(name, duration, count):
    print(f"\n=== {name}: outage {duration}s; injecting {count} events ===")
    req(EDGE+"/network","POST",{"online":False})
    before=req(EDGE+"/metrics")
    start=time.time()
    ids=[]
    for i in range(count):
        eid=str(uuid.uuid4()); ids.append(eid)
        req(EDGE+"/publish","POST",{"event_id":eid,"twin_id":f"trial-{name}",
             "source":"edge","value":20+i/10,"event_ts":time.time()})
        time.sleep(max(duration/count, .01))
    time.sleep(max(0,duration-(time.time()-start)))
    during=req(EDGE+"/metrics")
    offline_pending=during["pending_rows"]
    req(EDGE+"/network","POST",{"online":True})
    recovered=time.time()
    ok=wait_for(lambda: req(EDGE+"/metrics")["pending_rows"]==0, timeout=60)
    latency=round(time.time()-recovered,3) if ok else None
    cloud_ok=wait_for(lambda: any(x["twin_id"]==f"trial-{name}" for x in req(CLOUD+"/state")["state"]),timeout=20)
    after=req(EDGE+"/metrics")
    loss=max(0,len(ids)-offline_pending) # proxy for evicted/missing from this trial, not global exact count
    print(json.dumps({"trial":name,"outage_seconds":duration,"injected_events":len(ids),
      "pending_at_reconnect":offline_pending,"buffer_evictions_total":after["stats"]["evicted"]-before["stats"]["evicted"],
      "reconciliation_latency_seconds":latency,"pending_drained":ok,
      "cloud_state_seen":cloud_ok,"estimated_trial_loss_lower_bound":loss},indent=2))
    return {"trial":name,"outage_seconds":duration,"injected_events":len(ids),
      "pending_at_reconnect":offline_pending,"buffer_evictions":after["stats"]["evicted"]-before["stats"]["evicted"],
      "reconciliation_latency_seconds":latency,"pending_drained":ok,"cloud_state_seen":cloud_ok}

if __name__=="__main__":
    if not wait_for(lambda:req(EDGE+"/health")["status"]=="ok") or not wait_for(lambda:req(CLOUD+"/health")["status"]=="ok"):
        raise SystemExit("Start services first: docker compose up --build -d")
    results=[]
    results.append(trial("short",3,5))
    results.append(trial("medium",10,20))
    # Deliberately exceeds the default 500-row capacity by sending 520 quickly.
    print("\nLong trial sends 520 events while offline to exercise the bounded buffer.")
    req(EDGE+"/network","POST",{"online":False})
    before=req(EDGE+"/metrics")
    start=time.time()
    for i in range(520):
        req(EDGE+"/publish","POST",{"event_id":str(uuid.uuid4()),"twin_id":"trial-long",
             "source":"edge","value":25+i/100,"event_ts":time.time()})
    time.sleep(2)
    during=req(EDGE+"/metrics")
    req(EDGE+"/network","POST",{"online":True})
    t=time.time()
    drained=wait_for(lambda:req(EDGE+"/metrics")["pending_rows"]==0,timeout=90)
    results.append({"trial":"long","outage_seconds":round(time.time()-start,2),
      "injected_events":520,"pending_at_reconnect":during["pending_rows"],
      "buffer_evictions":req(EDGE+"/metrics")["stats"]["evicted"]-before["stats"]["evicted"],
      "reconciliation_latency_seconds":round(time.time()-t,3) if drained else None,
      "pending_drained":drained})
    print("\nSUMMARY\n"+json.dumps(results,indent=2))
