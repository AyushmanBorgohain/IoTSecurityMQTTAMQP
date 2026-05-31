import asyncio
from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived
from aioquic.asyncio.protocol import QuicConnectionProtocol

#Configuration
BROKER_IP = "10.0.2.8"
QUIC_TARGET_PORT = 5673
LOCAL_LISTEN_PORT = 5672

ACTIVE_SESSIONS = {}

#Used for establishing Client connection with the local proxy tunnel for sending AMQP packets over QUIC, which is not traditionally supported 

class BidirectionalClientProxy(QuicConnectionProtocol):
    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            writer = ACTIVE_SESSIONS.get(event.stream_id)
            if writer and not writer.is_closing():
                if event.data:
                    writer.write(event.data)
                if event.end_stream:
                    writer.close()

async def handle_local_amqp(local_reader, local_writer, quic_protocol):
    stream_id = quic_protocol._quic.get_next_available_stream_id()
    ACTIVE_SESSIONS[stream_id] = local_writer
    print(f"[+] Routing AMQP to QUIC Stream {stream_id}...")
    
    try:
        while True:
            # THE FIX: Read in 16KB chunks to prevent OS UDP Buffer overflows
            data = await local_reader.read(16384) 
            if not data:
                quic_protocol._quic.send_stream_data(stream_id, b"", end_stream=True)
                quic_protocol.transmit()
                break
            
            quic_protocol._quic.send_stream_data(stream_id, data, end_stream=False)
            quic_protocol.transmit()
            if len(data)>4096:
               await asyncio.sleep(0.002) 
            
    except Exception as e:
        print(f"[-] Proxy Error: {e}")
    finally:
        local_writer.close()
        if stream_id in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[stream_id]

async def keep_alive_ping(quic_protocol):
    try:
        while True:
            await asyncio.sleep(20.0)
            quic_protocol._quic.send_ping()
            quic_protocol.transmit()
    except asyncio.CancelledError: pass

async def main():
    configuration = QuicConfiguration(
        is_client=True, 
        verify_mode=True, 
        alpn_protocols=["amqp-proxy"], 
        idle_timeout=3600.0,
        max_data=1073741824,           
        max_stream_data=1073741824     
    )
    configuration.load_verify_locations("quic_cert.pem")
    print(f"[*] Connecting Tunnel to {BROKER_IP}:{QUIC_TARGET_PORT}...")
    
    async with connect(BROKER_IP, QUIC_TARGET_PORT, configuration=configuration, create_protocol=BidirectionalClientProxy) as quic_protocol:
        asyncio.create_task(keep_alive_ping(quic_protocol))
        print(f"[*] Listening on TCP {LOCAL_LISTEN_PORT}...")
        server = await asyncio.start_server(lambda r, w: handle_local_amqp(r, w, quic_protocol), '127.0.0.1', LOCAL_LISTEN_PORT)
        async with server: await server.serve_forever()

if __name__ == "__main__": asyncio.run(main())
