import asyncio
import websockets
import json
import pika
from message_queue_client import MessageQueueClient

uri = "wss://price-azu-01.vndirect.com.vn/realtime/websocket"

async def hello(queue_client, uri, message_name, codes):
    async with websockets.connect(uri) as websocket:
        example = { "type": 'registConsumer', "data": { "params": { "name": message_name, "codes": codes } } }
        await websocket.send(json.dumps(example))
        while True:
            greeting = await websocket.recv()
            queue_client.add_to_queue(greeting)

if __name__ == "__main__":
    message_queue_client = MessageQueueClient("localhost", 5673)
    asyncio.get_event_loop().run_until_complete(hello(message_queue_client, uri, "STOCK", ["VCB", "VIC"]))
    