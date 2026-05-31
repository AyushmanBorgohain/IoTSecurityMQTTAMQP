import tkinter as tk
from tkinter import scrolledtext
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import threading
from datetime import datetime

#Configuration
BROKER_IP = "10.0.2.8"
TOPIC = "sensor/temperature"
EMQX_PORT = 1883       # TCP/QUIC messages arrive here
RABBIT_PORT = 1884     # AMQP messages arrive here 


def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    timestamp = datetime.now().strftime("%H:%M:%S")

    clean_message = payload
    is_quic = False
    is_amqpt = False
    is_amqpq = False

    if payload.startswith("QUIC_TAG:") or payload.startswith("QUI_TAG:"):
    	protocol = "MQTT (QUIC)"
    	is_quic=True
    	clean_message = payload.split(":", 1)[1]
    elif payload.startswith("TCP_TAG:"):
        protocol = "MQTT (TCP)"
        clean_message = payload.split(":", 1)[1]
    elif payload.startswith("AMQT_TAG:"):
        protocol = "AMQP (TCP)"
        is_amqpt = True
        clean_message = payload.split(":", 1)[1]
    elif payload.startswith("AMQQ_TAG:"):
        protocol = "AMQP (QUIC)"
        is_amqpq = True
        clean_message = payload.split(":", 1)[1]        

    display_msg = f"[{protocol}] {clean_message}\n"

    chat_box.configure(state='normal')
    chat_box.insert(tk.END, display_msg)

    if is_quic:
        chat_box.tag_add("quic_style", "end-2c linestart", "end-1c")
        chat_box.tag_config("quic_style", foreground="blue", font=("Consolas", 10, "bold"))

    elif is_amqpt:
        chat_box.tag_add("amqpt_style", "end-2c linestart", "end-1c")
        chat_box.tag_config("amqpt_style", foreground="green", font=("Consolas", 10, "bold"))
        
    elif is_amqpq:
        chat_box.tag_add("amqpq_style", "end-2c linestart", "end-1c")
        chat_box.tag_config("amqpq_style", foreground="red", font=("Consolas", 10, "bold"))        

    chat_box.configure(state='disabled')
    chat_box.see(tk.END)


def start_emqx_client():
    """Connects to EMQX — receives TCP (TLS) and QUIC messages."""
    client = mqtt.Client(CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.on_message = on_message
    try:
        client.connect(BROKER_IP, EMQX_PORT, 60)
        client.subscribe(TOPIC)
        client.loop_forever()
    except Exception as e:
        print(f"[EMQX] Connection failed: {e}")


def start_rabbit_client():
    """Connects to RabbitMQ MQTT bridge — receives AMQP messages."""
    client = mqtt.Client(CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
    client.username_pw_set("admin", "password")
    client.on_message = on_message
    try:
        client.connect(BROKER_IP, RABBIT_PORT, 60)
        client.subscribe(TOPIC)
        client.loop_forever()
    except Exception as e:
        print(f"[RabbitMQ] Connection failed: {e}")


#Simple GUI commands for Tkinter
root = tk.Tk()
root.title("IoT Subscriber")
root.geometry("600x450")
root.configure(bg="#2c3e50")

header = tk.Label(root, text="MQTT LIVE SUBSCRIBER FEED",
                  font=("Helvetica", 16, "bold"),
                  bg="#2c3e50", fg="#ecf0f1", pady=10)
header.pack()

chat_box = scrolledtext.ScrolledText(root, width=70, height=20, font=("Consolas", 10))
chat_box.pack(padx=20, pady=10)
chat_box.configure(state='disabled', bg="#fdfdfd")

status_footer = tk.Label(root,
                         text=f"Listening on {BROKER_IP} | EMQX:{EMQX_PORT}  RabbitMQ:{RABBIT_PORT} | Topic: {TOPIC}",
                         font=("Helvetica", 9), bg="#2c3e50", fg="#bdc3c7")
status_footer.pack(pady=5)

# Two background threads — one per broker
threading.Thread(target=start_emqx_client, daemon=True).start()
threading.Thread(target=start_rabbit_client, daemon=True).start()

root.mainloop()
