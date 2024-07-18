import json
from quixstreams import Application


def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        consumer_group= "weather_reader",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["weather_data_demo"])
        while True:
            msg = consumer.poll(1)
            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode('utf8')
                value = json.loads(msg.value().decode('utf8'))
                offset = msg.offset()
                print(f"offset: {offset} key: {key} value: {value}")

if __name__ == "__main__":
    main()