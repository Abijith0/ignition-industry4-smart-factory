import paho.mqtt.client as mqtt
import sparkplug_b_pb2
import csv
import time
import os
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

print("AI EDGE PREDICTIVE MAINTENANCE SERVICE STARTED")

# MQTT SETTINGS
BROKER = "localhost"
PORT = 1883
TOPIC = "spBv1.0/#"

# ALERT TOPIC
ALERT_TOPIC = "factory/ai/alerts"

# CSV FILE
DATA_FILE = "machine_data.csv"

# MACHINE VARIABLES
start = 0
motor = 0
stop = 0
pv = 0
cv = 0

last_cycle_time = None
cycle_times = []

# ALERT CONTROL
last_alert_time = 0
ALERT_COOLDOWN = 60

# ML MODEL
model = IsolationForest(contamination=0.1)

# FEATURE HISTORY
feature_history = []

# CREATE CSV
if not os.path.exists(DATA_FILE):
    with open(DATA_FILE,"w",newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "start",
            "motor",
            "stop",
            "pv",
            "cv"
        ])

# MQTT ALERT CLIENT
alert_client = mqtt.Client(client_id="AI_ALERT_PUBLISHER")
alert_client.connect(BROKER,PORT)
alert_client.loop_start()


# ALERT FUNCTION
def send_alert(message):

    global last_alert_time

    now = time.time()

    if now-last_alert_time > ALERT_COOLDOWN:

        print("AI ALERT SENT:",message)

        alert_client.publish(ALERT_TOPIC,message)

        last_alert_time = now


# MACHINE HEALTH
def machine_health(df):

    if len(df)<5:
        return 100

    cv_std = df["cv"].std()

    cycle_std = np.std(cycle_times) if cycle_times else 0

    health = 100 - (cv_std*2 + cycle_std*5)

    return max(0,min(100,health))


# FEATURE GENERATION
def create_features(df):

    if len(df)<10:
        return None

    features = {}

    features["motor_mean"] = df["motor"].mean()
    features["motor_std"] = df["motor"].std()

    features["cv_mean"] = df["cv"].mean()
    features["cv_std"] = df["cv"].std()

    features["pv_mean"] = df["pv"].mean()
    features["pv_std"] = df["pv"].std()

    if cycle_times:
        features["cycle_time_mean"] = np.mean(cycle_times)
        features["cycle_time_std"] = np.std(cycle_times)
    else:
        features["cycle_time_mean"] = 0
        features["cycle_time_std"] = 0

    return features


# MQTT CONNECT
def on_connect(client,userdata,flags,rc):

    print("Connected to MQTT Broker")

    client.subscribe(TOPIC)


# MQTT MESSAGE
def on_message(client,userdata,msg):

    global start,motor,stop,pv,cv,last_cycle_time

    if "DDATA" not in msg.topic:
        return

    payload = sparkplug_b_pb2.Payload()
    payload.ParseFromString(msg.payload)

    for metric in payload.metrics:

        name = metric.name

        if name == "start":
            start = int(metric.boolean_value)

        elif name == "MOTOR":
            motor = int(metric.boolean_value)

        elif name == "STOP":
            stop = int(metric.boolean_value)

        elif name == "MOTOR_ON/PV":
            pv = int(metric.int_value)

        elif name == "MOTOR_ON/CV":
            cv = int(metric.int_value)

    timestamp = time.time()

    # LOG DATA
    with open(DATA_FILE,"a",newline="") as f:

        writer = csv.writer(f)

        writer.writerow([
            timestamp,
            start,
            motor,
            stop,
            pv,
            cv
        ])

    # CYCLE TIME
    if motor == 1:

        if last_cycle_time is None:
            last_cycle_time = timestamp

        else:

            cycle = timestamp-last_cycle_time

            cycle_times.append(cycle)

            last_cycle_time = timestamp

    # LOAD DATA
    df = pd.read_csv(DATA_FILE)

    # ANALYZE EVERY 10 ROWS
    if len(df)%10!=0:
        return

    print()
    print("Cycle Analysis")

    if cycle_times:

        avg_cycle = np.mean(cycle_times)
        latest_cycle = cycle_times[-1]

        print("Average:",round(avg_cycle,2))
        print("Latest:",round(latest_cycle,2))

        if latest_cycle > avg_cycle*1.5:

            print("⚠ MACHINE SLOWDOWN DETECTED")

            send_alert("Machine slowdown detected")

        else:
            print("Cycle normal")

    # MACHINE HEALTH
    health = machine_health(df)

    print()
    print("Machine Health:",round(health,2),"%")

    if health>90:
        print("Status: Healthy")

    elif health>60:
        print("Status: Warning")

    else:

        print("⚠ Preventive Maintenance Required")

        send_alert("Machine health critical")

    # CREATE FEATURES
    features = create_features(df)

    if features is None:
        return

    feature_history.append(features)

    # TRAIN ML WHEN ENOUGH DATA
    if len(feature_history)<10:
        return

    feature_df = pd.DataFrame(feature_history)

    try:

        model.fit(feature_df)

        prediction = model.predict(feature_df.tail(1))

        if prediction[0] == -1:

            print()
            print("⚠ ML MODEL DETECTED MACHINE ANOMALY")

            send_alert("AI detected abnormal machine behaviour")

        else:

            print()
            print("ML Prediction: Normal")

    except:
        pass


# MQTT CLIENT
client = mqtt.Client(client_id="AI_EDGE_SERVICE")

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER,PORT)

client.loop_forever()