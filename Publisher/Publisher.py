import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
import subprocess
import time
import os
import threading
import ssl
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import pika
import json
import random
import tempfile

#Defining properties of VMs
BROKER_IP = "10.0.2.8"
MY_VM_IP = "10.0.2.101"
TOPIC = "sensor/temperature"
AMQP_ROUTING_KEY = "sensor.temperature"
CA_CERT = "/home/vboxuser/prod_ca.crt"

LOCAL_QUIC_PROXY_PORT = 5672


def make_tag(tag_prefix, payload):
    return f"{tag_prefix}:{MY_VM_IP}|{time.time():.6f}|{payload}"

#Generate 60KB payload for stress-testing of the protocols
def generate_heavy_payload():
    payload = {
        "device_id": "edge_node_alpha_01",
        "timestamp_ms": time.time() * 1000,
        "status": "active",
        "thermals": {
            "cpu_temp_c": round(random.uniform(45.0, 85.0), 2),
            "gpu_temp_c": round(random.uniform(50.0, 90.0), 2),
            "ambient_temp_c": 25.5,
            "fan_speed_rpm": random.randint(1200, 3500)
        },
        "edge_inference": {
            "model": "vgg16_quantized_high_res",
            "target_class": random.choice(["pedestrian", "vehicle", "anomaly"]),
            "confidence_score": round(random.uniform(0.75, 0.99), 4),
            "inference_time_ms": random.randint(12, 45)
        },
        "sensor_buffer_history": [round(random.uniform(20.0, 35.0), 4) for _ in range(1500)],
        "vibration_fft_coefficients": [round(random.random(), 6) for _ in range(1500)]
    }
    return json.dumps(payload)

#Sequential testing payload creation function
def generate_small_payload():
    return json.dumps({"t": round(random.uniform(20.0, 35.0), 1)})

#Function for Publishing the message to the broker from the publisher 
def execute_publish(message, app_protocol, transport_protocol, is_ddos=False):
    unique_id = f"ryan_{int(time.time() * 1000)}"
    success = False

    if app_protocol == "MQTT":
        if transport_protocol == "TCP/TLS":
            tagged_message = make_tag("TCP_TAG", message)
            cmd = (f'mosquitto_pub -h {BROKER_IP} -p 8883 -t "{TOPIC}" '
                   f'-m \'{tagged_message}\' -V 5 --cafile {CA_CERT} -i "{unique_id}"')
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            success = (result.returncode == 0)

        elif transport_protocol == "QUIC":
            tagged_message = make_tag("QUI_TAG", message)
            cmd = (f'nanomq_cli pub --quic -h {BROKER_IP} -p 14567 '
                   f'-t "{TOPIC}" -m \'{tagged_message}\' -V 5 -i "{unique_id}" '
                   f'-q 1 -c true -k 60')
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            success = (result.returncode == 0) or ("connect" in result.stdout.lower())

    elif app_protocol == "AMQP":
        if transport_protocol == "TCP/TLS":
            tagged_message = make_tag("AMQT_TAG", message)
            cmd = (f'amqp-publish --url=amqp://admin:password@{BROKER_IP}:5672/%2f '
                   f'-e "amq.topic" -r "{AMQP_ROUTING_KEY}" -b \'{tagged_message}\'')
        elif transport_protocol == "QUIC":
            tagged_message = make_tag("AMQQ_TAG", message)
            cmd = (f'amqp-publish --url=amqp://admin:password@127.0.0.1:'
                   f'{LOCAL_QUIC_PROXY_PORT}/%2f '
                   f'-e "amq.topic" -r "{AMQP_ROUTING_KEY}" -b \'{tagged_message}\'')

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        success = (result.returncode == 0)

    if not is_ddos:
        if success:
            status_label.config(text=f"Success! {app_protocol}/{transport_protocol} ID: {unique_id[-4:]}", fg="green")
        else:
            status_label.config(text="Command failed.", fg="red")

#To send single message using the simple publish button
def send_single_message():
    message = msg_entry.get()
    if not message:
        messagebox.showwarning("Warning", "Please enter a message to send!")
        return
    status_label.config(text=f"Sending via {app_protocol_var.get()} over {transport_protocol_var.get()}...", fg="blue")
    root.update()
    threading.Thread(
        target=execute_publish,
        args=(message, app_protocol_var.get(), transport_protocol_var.get(), False),
        daemon=True
    ).start()

#To launch the DoS attack comprising of heavy payloads and fast transfers
def ddos_mqtt_tls(index, timing_results, lock):
    tagged = make_tag("TCP_TAG", generate_heavy_payload())
    try:
        start = time.time()
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5,
            client_id=f"atk_tls_{index}_{int(time.time()*1000)}"
        )
        context = ssl.create_default_context(cafile=CA_CERT)
        context.check_hostname = False
        client.tls_set_context(context)
        connected = threading.Event()
        published = threading.Event()
        client.on_connect = lambda c, u, f, rc, p=None: connected.set() if rc == 0 else None
        client.on_publish = lambda c, u, mid, rc=None, p=None: published.set()
        client.connect(BROKER_IP, 8883, keepalive=5)
        client.loop_start()
        connected.wait(timeout=10)
        if connected.is_set():
            client.publish(TOPIC, tagged, qos=1)
            published.wait(timeout=10)
        elapsed = (time.time() - start) * 1000
        client.loop_stop()
        client.disconnect()
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception as e:
        print(f"[TLS ATTACK {index}] Error: {e}")

#Same thing over QUIC as above 
def ddos_mqtt_quic(index, timing_results, lock):
    tagged = make_tag("QUI_TAG", generate_heavy_payload())
    unique_id = f"quic_atk_{index}_{int(time.time()*1000)}"
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt')
    try:
        temp_file.write(tagged)
        temp_file.close()
        start = time.time()
        cmd = (f"nanomq_cli pub --quic -h {BROKER_IP} -p 14567 "
               f"-t '{TOPIC}' -f {temp_file.name} -V 5 -i '{unique_id}' -q 1 -c true -k 5")
        subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=20)
        elapsed = (time.time() - start) * 1000
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception as e:
        print(f"[QUIC ATTACK {index}] Error: {e}")
    finally:
        os.unlink(temp_file.name)


def ddos_amqp_tcp(index, timing_results, lock):
    tagged = make_tag("AMQT_TAG", generate_heavy_payload())
    try:
        start = time.time()
        credentials = pika.PlainCredentials("admin", "password")
        params = pika.ConnectionParameters(
            host=BROKER_IP, port=5672, credentials=credentials,
            connection_attempts=3, retry_delay=1, socket_timeout=20
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.basic_publish(exchange="amq.topic", routing_key=AMQP_ROUTING_KEY, body=tagged)
        connection.close()
        elapsed = (time.time() - start) * 1000
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception as e:
        print(f"[AMQP TCP ATTACK {index}] Error: {e}")


def ddos_amqp_quic(index, timing_results, lock):
    # THE FIX: Slightly wider stagger (40ms) to ensure OS UDP buffer drops are avoided.
    time.sleep(index * 0.04) 
    tagged = make_tag("AMQQ_TAG", generate_heavy_payload())
    try:
        start = time.time()
        credentials = pika.PlainCredentials("admin", "password")
        # THE FIX: Bumped timeout to 60s and disabled heartbeat so Pika survives the paced proxy pipeline.
        params = pika.ConnectionParameters(
            host="127.0.0.1", port=LOCAL_QUIC_PROXY_PORT, credentials=credentials,
            connection_attempts=3, retry_delay=1, socket_timeout=60, heartbeat=0
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.basic_publish(exchange="amq.topic", routing_key=AMQP_ROUTING_KEY, body=tagged)
        connection.close()
        elapsed = (time.time() - start) * 1000
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception as e:
        print(f"[AMQP QUIC ATTACK {index}] Error: {e}")

#Small payload for the stated protocols below
def _small_mqtt_tls(index, timing_results, lock):
    tagged = make_tag("TCP_TAG", generate_small_payload())
    try:
        start = time.time()
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5,
            client_id=f"sm_tls_{index}_{int(time.time()*1000)}"
        )
        context = ssl.create_default_context(cafile=CA_CERT)
        context.check_hostname = False
        client.tls_set_context(context)
        connected = threading.Event()
        published = threading.Event()
        client.on_connect = lambda c, u, f, rc, p=None: connected.set() if rc == 0 else None
        client.on_publish = lambda c, u, mid, rc=None, p=None: published.set()
        client.connect(BROKER_IP, 8883, keepalive=5)
        client.loop_start()
        connected.wait(timeout=10)
        if connected.is_set():
            client.publish(TOPIC, tagged, qos=1)
            published.wait(timeout=10)
        elapsed = (time.time() - start) * 1000
        client.loop_stop()
        client.disconnect()
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception:
        pass


def _small_mqtt_quic(index, timing_results, lock):
    tagged = make_tag("QUI_TAG", generate_small_payload())
    unique_id = f"sm_qui_{index}_{int(time.time()*1000)}"
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt')
    try:
        temp_file.write(tagged)
        temp_file.close()
        start = time.time()
        cmd = (f"nanomq_cli pub --quic -h {BROKER_IP} -p 14567 "
               f"-t '{TOPIC}' -f {temp_file.name} -V 5 -i '{unique_id}' -q 1 -c true -k 5")
        subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        elapsed = (time.time() - start) * 1000
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception:
        pass
    finally:
        os.unlink(temp_file.name)


def _small_amqp_tcp(index, timing_results, lock):
    tagged = make_tag("AMQT_TAG", generate_small_payload())
    try:
        start = time.time()
        credentials = pika.PlainCredentials("admin", "password")
        params = pika.ConnectionParameters(
            host=BROKER_IP, port=5672, credentials=credentials,
            connection_attempts=3, retry_delay=1, socket_timeout=10
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.basic_publish(exchange="amq.topic", routing_key=AMQP_ROUTING_KEY, body=tagged)
        connection.close()
        elapsed = (time.time() - start) * 1000
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception:
        pass


def _small_amqp_quic(index, timing_results, lock):
    time.sleep(index * 0.01) 
    tagged = make_tag("AMQQ_TAG", generate_small_payload())
    try:
        start = time.time()
        credentials = pika.PlainCredentials("admin", "password")
        params = pika.ConnectionParameters(
            host="127.0.0.1", port=LOCAL_QUIC_PROXY_PORT, credentials=credentials,
            connection_attempts=3, retry_delay=1, socket_timeout=30, heartbeat=0
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.basic_publish(exchange="amq.topic", routing_key=AMQP_ROUTING_KEY, body=tagged)
        connection.close()
        elapsed = (time.time() - start) * 1000
        with lock:
            timing_results.append({"total_ms": elapsed})
    except Exception:
        pass


def get_attack_fn(app_protocol, transport_protocol):
    if app_protocol == "MQTT" and transport_protocol == "TCP/TLS":   return ddos_mqtt_tls
    elif app_protocol == "MQTT" and transport_protocol == "QUIC":    return ddos_mqtt_quic
    elif app_protocol == "AMQP" and transport_protocol == "TCP/TLS": return ddos_amqp_tcp
    elif app_protocol == "AMQP" and transport_protocol == "QUIC":    return ddos_amqp_quic
    return None

#Launch the DoS
def launch_ddos():
    app, trans = app_protocol_var.get(), transport_protocol_var.get()
    attack_fn = get_attack_fn(app, trans)
    if not attack_fn:
        return
    status_label.config(text=f"SINGLE BURST: {app} over {trans}", fg="red")
    root.update()

    def attack_loop():
        timing_results, lock, threads = [], threading.Lock(), []
        for i in range(50):
            t = threading.Thread(target=attack_fn, args=(i, timing_results, lock), daemon=True)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        if timing_results:
            totals = [r["total_ms"] for r in timing_results]
            print(f"\n{'='*55}")
            print(f"Single Burst Result: {app} over {trans}")
            print(f"  Packets sent:          {len(totals)}")
            print(f"  Avg latency:           {sum(totals)/len(totals):.1f} ms")
            print(f"  Min / Max:             {min(totals):.1f} / {max(totals):.1f} ms")
            print(f"{'='*55}\n")
        status_label.config(text="Burst attack finished.", fg="orange")

    threading.Thread(target=attack_loop, daemon=True).start()


def launch_small_payload_test():
    status_label.config(text="Running full matrix payload framing test...", fg="purple")
    root.update()

    def test_loop():
        m_tls_res, m_qui_res, a_tcp_res, a_qui_res = [], [], [], []
        l_mt, l_mq, l_at, l_aq = threading.Lock(), threading.Lock(), threading.Lock(), threading.Lock()
        
        # 1. Test MQTT over TCP/TLS
        print("[*] Benchmarking MQTT/TLS...")
        threads = [threading.Thread(target=_small_mqtt_tls, args=(i, m_tls_res, l_mt), daemon=True) for i in range(50)]
        for t in threads: t.start()
        for t in threads: t.join()

        # 2. Test MQTT over QUIC
        print("[*] Benchmarking MQTT/QUIC...")
        threads = [threading.Thread(target=_small_mqtt_quic, args=(i, m_qui_res, l_mq), daemon=True) for i in range(50)]
        for t in threads: t.start()
        for t in threads: t.join()

        # 3. Test AMQP over TCP
        print("[*] Benchmarking AMQP/TCP...")
        threads = [threading.Thread(target=_small_amqp_tcp, args=(i, a_tcp_res, l_at), daemon=True) for i in range(50)]
        for t in threads: t.start()
        for t in threads: t.join()

        # 4. Test AMQP over QUIC
        print("[*] Benchmarking AMQP/QUIC...")
        threads = [threading.Thread(target=_small_amqp_quic, args=(i, a_qui_res, l_aq), daemon=True) for i in range(50)]
        for t in threads: t.start()
        for t in threads: t.join()

        m_tls_avg = sum(r["total_ms"] for r in m_tls_res) / len(m_tls_res) if m_tls_res else 0
        m_qui_avg = sum(r["total_ms"] for r in m_qui_res) / len(m_qui_res) if m_qui_res else 0
        a_tcp_avg = sum(r["total_ms"] for r in a_tcp_res) / len(a_tcp_res) if a_tcp_res else 0
        a_qui_avg = sum(r["total_ms"] for r in a_qui_res) / len(a_qui_res) if a_qui_res else 0

        print(f"\n{'='*55}")
        print(f"Full Matrix Framing Test  (50 msgs @ ~20 bytes)")
        print(f"  MQTT/TLS avg:   {m_tls_avg:.1f} ms")
        print(f"  MQTT/QUIC avg:  {m_qui_avg:.1f} ms  <-- (Inflated heavily by nanomq_cli OS spawn overhead)")
        print(f"  AMQP/TCP avg:   {a_tcp_avg:.1f} ms")
        print(f"  AMQP/QUIC avg:  {a_qui_avg:.1f} ms")
        print(f"{'='*55}\n")
        status_label.config(text="Matrix framing test complete.", fg="purple")

    threading.Thread(target=test_loop, daemon=True).start()

#Function for netem i.e. adding artificial delays and packet losses to simulate real-life
def apply_netem():
    iface = netem_iface_var.get().strip()
    delay = netem_delay_var.get().strip()
    loss  = netem_loss_var.get().strip()
    if not iface:
        messagebox.showwarning("Warning", "Enter a network interface name (e.g. enp0s3)")
        return

    cmd = f"sudo tc qdisc add dev {iface} root netem"
    if delay:
        cmd += f" delay {delay}ms 5ms distribution normal"
    if loss:
        cmd += f" loss {loss}%"

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        subprocess.run(f"sudo tc qdisc del dev {iface} root", shell=True, capture_output=True)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        status_label.config(text=f"Netem applied: {delay}ms delay, {loss}% loss on {iface}", fg="darkorange")
    else:
        status_label.config(text=f"Netem error — check sudo/interface. {result.stderr.strip()}", fg="red")


def remove_netem():
    iface = netem_iface_var.get().strip()
    if not iface:
        messagebox.showwarning("Warning", "Enter a network interface name first")
        return
    result = subprocess.run(f"sudo tc qdisc del dev {iface} root", shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        status_label.config(text=f"Netem removed from {iface}. Network restored.", fg="green")
    else:
        status_label.config(text="Nothing to remove — network already clean.", fg="green")

#Basic tkinter GUI for the Publisher 
root = tk.Tk()
root.title("IoT Publisher Dashboard (Admin Mode)")
root.geometry("480x800")
root.configure(padx=20, pady=15)

tk.Label(root, text="Multi-Protocol Payload Generator", font=("Helvetica", 14, "bold")).pack(pady=5)

tk.Label(root, text="Enter JSON Payload (For Normal Publish):").pack(anchor="w")
msg_entry = tk.Entry(root, width=50)
msg_entry.insert(0, '{"temperature_c": 25.5}')
msg_entry.pack(pady=5)

tk.Label(root, text="Select Application Protocol:").pack(anchor="w", pady=(5, 0))
app_protocol_var = tk.StringVar(value="MQTT")
ttk.Combobox(root, textvariable=app_protocol_var, values=["MQTT", "AMQP"], state="readonly").pack(fill="x", pady=5)

tk.Label(root, text="Select Transport Layer:").pack(anchor="w", pady=(5, 0))
transport_protocol_var = tk.StringVar(value="TCP/TLS")
tk.Radiobutton(root, text="TCP/TLS (Standard/Legacy)", variable=transport_protocol_var, value="TCP/TLS").pack(anchor="w")
tk.Radiobutton(root, text="QUIC (0-RTT UDP)",          variable=transport_protocol_var, value="QUIC").pack(anchor="w")

tk.Button(root, text="NORMAL PUBLISH", bg="#4CAF50", fg="white", font=("Helvetica", 10, "bold"), command=send_single_message).pack(pady=(15, 5), fill="x")
tk.Button(root, text="SINGLE BURST ATTACK  (~60 KB × 50 threads)", bg="#f57c00", fg="white", font=("Helvetica", 10, "bold"), command=launch_ddos).pack(pady=5, fill="x")
tk.Button(root, text="FULL MATRIX FRAMING TEST  (~20 B × 50 msgs)", bg="#7b1fa2", fg="white", font=("Helvetica", 10, "bold"), command=launch_small_payload_test).pack(pady=5, fill="x")

netem_frame = tk.LabelFrame(root, text="Network Impairment Simulator (tc netem)", padx=10, pady=8)
netem_frame.pack(fill="x", pady=(14, 5))

netem_iface_var = tk.StringVar(value="enp0s3")
netem_delay_var = tk.StringVar(value="30")
netem_loss_var  = tk.StringVar(value="5")

row1 = tk.Frame(netem_frame)
row1.pack(fill="x", pady=3)
tk.Label(row1, text="Interface:", width=10, anchor="w").pack(side="left")
tk.Entry(row1, textvariable=netem_iface_var, width=10).pack(side="left", padx=3)
tk.Label(row1, text="Delay (ms):", width=10, anchor="w").pack(side="left")
tk.Entry(row1, textvariable=netem_delay_var, width=5).pack(side="left", padx=3)
tk.Label(row1, text="Loss (%):", width=8, anchor="w").pack(side="left")
tk.Entry(row1, textvariable=netem_loss_var, width=5).pack(side="left", padx=3)

row2 = tk.Frame(netem_frame)
row2.pack(fill="x", pady=4)
tk.Button(row2, text="APPLY IMPAIRMENT", bg="#c62828", fg="white", font=("Helvetica", 9, "bold"), command=apply_netem).pack(side="left", expand=True, fill="x", padx=(0, 4))
tk.Button(row2, text="REMOVE IMPAIRMENT", bg="#2e7d32", fg="white", font=("Helvetica", 9, "bold"), command=remove_netem).pack(side="left", expand=True, fill="x")

tk.Label(netem_frame, text="Tip: apply on publisher VM to simulate real-world path impairment", font=("Helvetica", 7), fg="#888").pack()

status_label = tk.Label(root, text="Status: Ready", font=("Helvetica", 10, "italic"))
status_label.pack(pady=10)

tk.Label(root, text="B.Tech 2027 - IoT Networking Project", font=("Helvetica", 7), fg="#aaa").pack(side="bottom")

root.mainloop()
