import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import sqlite3
import time
from datetime import datetime
import os
import sys
from collections import deque

# Configuration
DB_NAME = "iot_telemetry.db"
EMQX_IP = "127.0.0.1"
EMQX_PORT = 1883
RABBIT_PORT = 1884
EMQX_TOPIC = "sensor/temperature"
RABBIT_TOPIC = "sensor/temperature"

DATARATE_THRESHOLD_BPS = 500.0
WINDOW_SECONDS = 1.0  # sliding window size

client_tracking = {}


def init_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.execute('DROP TABLE IF EXISTS logs')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           DATETIME,
            sender_ip           TEXT,
            protocol            TEXT,
            payload             TEXT,
            size_bytes          INTEGER,
            time_since_last_ms  INTEGER,
            datarate            REAL,
            e2e_latency_ms      REAL
        )
    ''')
    conn.commit()
    return conn


conn = init_db()
blocked_keys = set()  


def block_attacker(ip_address, protocol):
    print(f"\n{'='*50}")
    print(f"[IPS] DDoS threshold exceeded! IP: {ip_address} | Protocol: {protocol}")
    print(f"[IPS] Datarate exceeded {DATARATE_THRESHOLD_BPS} B/s — deploying iptables block...")
    os.system(f"sudo iptables -A INPUT -s {ip_address} -j DROP")
    print(f"[IPS] Traffic from {ip_address} successfully terminated.")
    print(f"{'='*50}\n")


def on_message(client, userdata, message):
    current_time = time.time()

    try:
        raw_payload = message.payload.decode("utf-8")

        if not any(raw_payload.startswith(tag) for tag in
                   ["QUIC_TAG:", "QUI_TAG:", "TCP_TAG:", "AMQQ_TAG:", "AMQT_TAG:"]):
            return

        tag_split = raw_payload.split(":", 1)[1]

        parts = tag_split.split("|", 2)
        if len(parts) == 3:
            sender_ip, send_ts_str, clean_payload = parts
            try:
                send_ts = float(send_ts_str)
                e2e_latency_ms = round((current_time - send_ts) * 1000, 3)
            except ValueError:
                send_ts = None
                e2e_latency_ms = None
        elif len(parts) == 2:
            sender_ip, clean_payload = parts
            e2e_latency_ms = None  
        else:
            return

        if "QUIC" in raw_payload or "QUI" in raw_payload:
            protocol = "QUIC"
        elif "TCP" in raw_payload:
            protocol = "TCP (TLS)"
        elif "AMQT" in raw_payload:
            protocol = "AMQP (TCP)"
        elif "AMQQ" in raw_payload:
            protocol = "AMQP (QUIC)"
        else:
            return

        payload_size = len(clean_payload.encode("utf-8"))

        track_key = (sender_ip, protocol)

        if track_key in blocked_keys:
            return 

        if track_key not in client_tracking:
            client_tracking[track_key] = deque()

        window = client_tracking[track_key]
        window.append((current_time, payload_size))
        while window and (current_time - window[0][0]) > WINDOW_SECONDS:
            window.popleft()

        # Rate = Total Bytes in Window / Rate Duration
        if len(window) >= 2:
            window_duration = window[-1][0] - window[0][0]
            datarate = (sum(s for _, s in window) / window_duration) if window_duration > 0 else 0.0
        else:
            datarate = 0.0

        # Time since last packet 
        elapsed_ms = int((window[-1][0] - window[-2][0]) * 1000) if len(window) >= 2 else 0

        # Commented as this is the unrestricted broker 
        '''if datarate > DATARATE_THRESHOLD_BPS:
            blocked_keys.add(track_key)
            block_attacker(sender_ip, protocol)
            return  # Drop this packet — don't log to DB after a block'''

        #Insert into Database (SQLite)
        conn.execute('''
            INSERT INTO logs
                (timestamp, sender_ip, protocol, payload,
                 size_bytes, time_since_last_ms, datarate, e2e_latency_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            sender_ip, protocol, clean_payload,
            payload_size, elapsed_ms, datarate, e2e_latency_ms
        ))
        conn.commit()

        latency_str = f"{e2e_latency_ms:8.2f} ms E2E" if e2e_latency_ms is not None else "  E2E: N/A (legacy)"
        print(f"[{sender_ip}] | {protocol:12} | {payload_size:6} B | "
              f"{elapsed_ms:5} ms interval | {datarate:9.2f} B/s | {latency_str}")

    except ValueError as e:
        print(f"[WARN] Malformed payload: {e}")
    except Exception as e:
        print(f"[ERROR] {e}")


# Connection Callbacks
def on_emqx_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[+] SUCCESS: Connected to EMQX Broker Engine.")
        client.subscribe(EMQX_TOPIC)
        print(f"[+] EMQX: Subscribed to topic '{EMQX_TOPIC}'")
    else:
        print(f"[-] ERROR: EMQX Connection Failed. RC: {rc}")


def on_rabbit_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[+] SUCCESS: Connected to RabbitMQ Broker Engine.")
        client.subscribe(RABBIT_TOPIC)
        print(f"[+] RabbitMQ: Subscribed to topic '{RABBIT_TOPIC}'")
    else:
        print(f"[-] ERROR: RabbitMQ Refused. RC: {rc}")


# Client definitions
client_emqx = mqtt.Client(CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
client_emqx.on_connect = on_emqx_connect
client_emqx.on_message = on_message

client_rabbit = mqtt.Client(CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
client_rabbit.on_connect = on_rabbit_connect
client_rabbit.on_message = on_message
client_rabbit.username_pw_set("admin", "password")

try:
    print("Unified IPS Telemetry Database Engine...")
    print(f"IPS Threshold: {DATARATE_THRESHOLD_BPS} B/s | Window: {WINDOW_SECONDS}s")
    print(f"Linking to EMQX Engine (TCP/QUIC) on port {EMQX_PORT}...")
    client_emqx.connect(EMQX_IP, EMQX_PORT)
    client_emqx.loop_start()

    print(f"Linking to RabbitMQ Engine (AMQP/MQTT Bridge) on port {RABBIT_PORT}...")
    client_rabbit.connect(EMQX_IP, RABBIT_PORT)

    print("\n[IPS Monitor Active. Scanning all network protocols...]\n")
    print(f"{'IP':<16} | {'Protocol':<12} | {'Size':>6} | {'Interval':>8} | {'Datarate':>12} | E2E Latency")
    print("-" * 85)
    client_rabbit.loop_forever()

except KeyboardInterrupt:
    print("\n[-] Shutting down IPS Monitor safely.")
except Exception as main_err:
    print(f"\n[-] Critical Core Failure: {main_err}")
finally:
    client_emqx.loop_stop()
    conn.close()
    print("[INFO] DB connection closed.")
    sys.exit(0)
