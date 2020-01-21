import asyncio
import websockets
import json
import pika
from message_queue_client import MessageQueueClient
from threading import Thread, Timer, Event

uri = "wss://price-azu-01.vndirect.com.vn/realtime/websocket"

class IntervalEvent(Thread):
    def __init__(self, interval : "Minutes", event, function, *args):
        Thread.__init__(self)
        self.stopped = event
        self.interval = interval
        self.function = function
        self.args = args

    def run(self):
        while not self.stopped.wait(self.interval):
            asyncio.run(self.function(*self.args))


async def ping_server(websocket):
    await websocket.send("ping")

async def hello(queue_client, uri, message_name, codes):
    async with websockets.connect(uri) as websocket:
        example = { "type": 'registConsumer', "data": { "params": { "name": message_name, "codes": codes } } }
        await websocket.send(json.dumps(example))
        stop_flag = Event()
        thread = IntervalEvent(5, stop_flag, ping_server, websocket)
        thread.start()
        while True:
            try:
                greeting = await websocket.recv()
                queue_client.add_to_queue(greeting)
            except KeyboardInterrupt:
                thread.join()
                thread.stopped.set()

if __name__ == "__main__":
    message_queue_client = MessageQueueClient("localhost", 5673)
    asyncio.ensure_future(hello(message_queue_client, uri, "STOCK", ["VCB", "VIC"]))
    asyncio.ensure_future(hello(message_queue_client, uri, "DERIVATIVE_OPT", ["VN30F2002"]))
    # asyncio.get_event_loop().run_until_complete(hello(message_queue_client, uri, "STOCK", ["VCB", "VIC"]))
    # asyncio.get_event_loop().run_until_complete(hello(message_queue_client, uri, "DERIVATIVE_OPT", ["VN30F2002"]))
    loop = asyncio.get_event_loop()
    loop.run_forever()