import pika
import json
from datetime import datetime
import pandas as pd
from database import StockDBAccessor
import numpy as np

class MessageQueueProcessingWorker:
    exchange_name = "streamed_data"
    def __init__(self, queue_server_ip="localhost", queue_server_port=5673):
        self.ip = queue_server_ip
        self.port = queue_server_port
        self.db = StockDBAccessor("localhost", 27017)
        self.dataframe = None
        params = pika.ConnectionParameters(host=queue_server_ip, heartbeat=600,
                                       blocked_connection_timeout=300)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.exchange_declare(self.exchange_name, "fanout")
        queue_name = self.temp_queue_declare(durable=True)
        self.channel.basic_consume(queue_name, on_message_callback=self.processing_data)
        self.channel.start_consuming()

    def temp_queue_declare(self, durable=False):
        result = self.channel.queue_declare(queue="", durable=durable, exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name)
        return queue_name


    def exchange_declare(self, exchange_name, exchange_type="fanout"):
        self.channel.exchange_declare(exchange=exchange_name,
                                        exchange_type=exchange_type)

    def processing_data(self, ch, method, properties, body):
        data = json.loads(body)
        inserted_data = self.db.creating_insert_datas(data)
        self.create_dataframe(inserted_data)
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def create_dataframe(self, inserted_data):
        for data in inserted_data:
            if self.dataframe is None:
                self.dataframe = pd.DataFrame({"DateTime" : [], data["item_name"] : []})
            if data["item_name"] not in self.dataframe.columns:
                self.dataframe[data["item_name"]] = np.nan

            inserted_time = np.datetime64(data["inserted_time"].replace(minute=(data["inserted_time"].minute//5)*5, second=0, microsecond=0))

            if inserted_time not in self.dataframe["DateTime"].values:
                self.dataframe = self.dataframe.append({"DateTime" : inserted_time, data["item_name"] : data["item_value"]}, ignore_index=True)
            elif not self.dataframe[(self.dataframe[data["item_name"]].isnull()) & (self.dataframe["DateTime"] == inserted_time)].empty:
                self.dataframe.loc[(self.dataframe[data["item_name"]].isnull()) & (self.dataframe["DateTime"] == inserted_time), data["item_name"]] = data["item_value"]
                print(self.dataframe)

if __name__ == "__main__":
    MessageQueueProcessingWorker()