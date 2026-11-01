import json, os, time, uuid, random
import paho.mqtt.client as mqtt

host=os.getenv("MQTT_HOST","localhost"); port=int(os.getenv("MQTT_PORT","1883"))
interval=float(os.getenv("SENSOR_INTERVAL_SECONDS","2"))
client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="simulated-sensor")
while True:
    try:
        client.connect(host,port,60); break
    except Exception:
        time.sleep(1)
client.loop_start()
print("Simulated sensor publishing to twin/machine-01/reading")
while True:
    event={"event_id":str(uuid.uuid4()),"twin_id":"machine-01","source":"edge",
           "value":round(random.uniform(18,32),2),"event_ts":time.time()}
    client.publish("twin/machine-01/reading",json.dumps(event),qos=1)
    print("Published",event,flush=True)
    time.sleep(interval)
