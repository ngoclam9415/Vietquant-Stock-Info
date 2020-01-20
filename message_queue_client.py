import pika

class MessageQueueClient:
    exchange_name = "streamed_data"
    def __init__(self, queue_server_ip="localhost", queue_server_port=5673):
        self.ip = queue_server_ip
        self.port = queue_server_port
        params = pika.ConnectionParameters(host=queue_server_ip, heartbeat=600,
                                       blocked_connection_timeout=300)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.exchange_declare(self.exchange_name, "fanout")

    def exchange_declare(self, exchange_name, exchange_type="fanout"):
        self.channel.exchange_declare(exchange=exchange_name,
                                        exchange_type=exchange_type)

    def add_to_queue(self, data):
        try:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key="", body=data, properties=pika.BasicProperties(
                                            delivery_mode=2,  # make message persistent
                                            ))
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.connection.close()
        except pika.exceptions.ConnectionClosedByBroker:
            pass
        except (pika.exceptions.AMQPChannelError,pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as err:
            print("Caught a error: {}, reconnecting...".format(err))
            super(MessageQueueClient, self).__init__(self.ip, self.port)