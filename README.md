## Dependencies

#### RabbitMQ
    sudo apt-get install rabbitmq-server
    sudo service rabbitmq-server start

#### MongoDB
Install using this [Link](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)


#### Python Package
    pip install -r requirement.txt

#### HOW TO RUN
Open 3 different terminal tab then

    python message_queue_parsing_worker.py
    python message_queue_processing_worker.py
    python streaming_data.py
