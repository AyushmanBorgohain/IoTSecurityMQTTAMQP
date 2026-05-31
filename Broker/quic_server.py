# quic_server.py (Anti-Stutter Paced Version - Run on Node 1)
import asyncio
from aioquic.asyncio import serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived, HandshakeCompleted, ConnectionTerminated
from aioquic.asyncio.protocol import QuicConnectionProtocol

RABBITMQ_HOST = '127.0.0.1'
RABBITMQ_PORT = 5672
QUIC_LISTEN_PORT = 5673

_END_OF_STREAM = object()

class AMQPSessionHandler:
    def __init__(self, stream_id, quic_protocol):
        self.stream_id = stream_id
        self.quic_protocol = quic_protocol
        self.data_queue: asyncio.Queue = asyncio.Queue()
        self.rabbitmq_writer: asyncio.StreamWriter | None = None
        self.rabbitmq_reader: asyncio.StreamReader | None = None
        self._write_task: asyncio.Task | None = None
        self._read_task: asyncio.Task | None = None

    async def start(self):
        try:
            self.rabbitmq_reader, self.rabbitmq_writer = await asyncio.open_connection(RABBITMQ_HOST, RABBITMQ_PORT)
            print(f"[+] Tunnel Linked: QUIC Stream {self.stream_id} -> RabbitMQ")

            self._write_task = asyncio.create_task(self._relay_to_rabbitmq())
            self._read_task = asyncio.create_task(self._relay_from_rabbitmq())

            done, pending = await asyncio.wait(
                [self._write_task, self._read_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                try: await task
                except: pass

        except ConnectionRefusedError:
            print(f"[-] RabbitMQ refused connection for Stream {self.stream_id}")
        except Exception as e:
            print(f"[-] Relay Error on Stream {self.stream_id}: {e}")
        finally:
            await self._cleanup()

    async def _relay_to_rabbitmq(self):
        try:
            while True:
                data = await self.data_queue.get()
                self.data_queue.task_done()
                if data is _END_OF_STREAM: break
                if self.rabbitmq_writer and not self.rabbitmq_writer.is_closing():
                    self.rabbitmq_writer.write(data)
                    await self.rabbitmq_writer.drain()
        except asyncio.CancelledError: pass
        except Exception as e: print(f"[-] Write Error: {e}")

    async def _relay_from_rabbitmq(self):
        try:
            while True:
                # THE FIX: 16KB anti-stutter chunks
                response = await self.rabbitmq_reader.read(16384) 
                if not response: break
                self.quic_protocol._quic.send_stream_data(self.stream_id, response, end_stream=False)
                self.quic_protocol.transmit()
                
                # THE PACER
                if len(response)>4096:
                   await asyncio.sleep(0.002) 
        except asyncio.CancelledError: pass
        except Exception as e: print(f"[-] Read Error: {e}")
        finally:
            try:
                self.quic_protocol._quic.send_stream_data(self.stream_id, b"", end_stream=True)
                self.quic_protocol.transmit()
            except Exception: pass

    def queue_data(self, data: bytes):
        self.data_queue.put_nowait(data)

    def signal_end_stream(self):
        self.data_queue.put_nowait(_END_OF_STREAM)

    async def _cleanup(self):
        for task in (self._write_task, self._read_task):
            if task and not task.done():
                task.cancel()
                try: await task
                except: pass
        if self.rabbitmq_writer:
            try:
                self.rabbitmq_writer.close()
                await self.rabbitmq_writer.wait_closed()
            except Exception: pass
        print(f"[*] Session {self.stream_id} finalized.")

class AMQPProxyProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_sessions: dict[int, AMQPSessionHandler] = {}

    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            print("[+] QUIC TLS 1.3 Handshake Completed")

        elif isinstance(event, ConnectionTerminated):
            print(f"[-] QUIC Connection Terminated")
            for session in list(self.active_sessions.values()):
                asyncio.create_task(session._cleanup())
            self.active_sessions.clear()

        elif isinstance(event, StreamDataReceived):
            sid = event.stream_id
            if sid not in self.active_sessions:
                handler = AMQPSessionHandler(sid, self)
                self.active_sessions[sid] = handler
                if event.data: handler.queue_data(event.data)
                if event.end_stream: handler.signal_end_stream()
                asyncio.create_task(handler.start())
                if event.end_stream: del self.active_sessions[sid]
            else:
                handler = self.active_sessions[sid]
                if event.data: handler.queue_data(event.data)
                if event.end_stream:
                    handler.signal_end_stream()
                    del self.active_sessions[sid]

async def main():
    configuration = QuicConfiguration(
        is_client=False, 
        alpn_protocols=["amqp-proxy"], 
        idle_timeout=3600.0,
        max_data=1073741824,           
        max_stream_data=1073741824     
    )
    configuration.load_cert_chain("quic_cert.pem", "quic_key.pem")
    print(f"[*] Starting Server on UDP {QUIC_LISTEN_PORT}...")
    await serve(host="0.0.0.0", port=QUIC_LISTEN_PORT, configuration=configuration, create_protocol=AMQPProxyProtocol)
    await asyncio.Future()

if __name__ == "__main__": asyncio.run(main())
