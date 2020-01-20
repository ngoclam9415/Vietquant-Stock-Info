import pika
import json

class MessageQueueProcessingWorker:
    exchange_name = "streamed_data"
    def __init__(self, queue_server_ip="localhost", queue_server_port=5673):
        self.ip = queue_server_ip
        self.port = queue_server_port
        params = pika.ConnectionParameters(host=queue_server_ip, heartbeat=600,
                                       blocked_connection_timeout=300)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.exchange_declare(self.exchange_name, "fanout")
        queue_name = self.temp_queue_declare(durable=True)
        self.channel.basic_consume(queue_name, on_message_callback=self.parsing_data)
        self.channel.start_consuming()

    def temp_queue_declare(self, durable=False):
        result = self.channel.queue_declare(queue="", durable=durable, exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name)
        return queue_name


    def exchange_declare(self, exchange_name, exchange_type="fanout"):
        self.channel.exchange_declare(exchange=exchange_name,
                                        exchange_type=exchange_type)

    def parsing_data(self, ch, method, properties, body):
        data = json.loads(body)
        print("PROCESSING : {}".format(data))
        ch.basic_ack(delivery_tag = method.delivery_tag)

if __name__ == "__main__":
    MessageQueueProcessingWorker()