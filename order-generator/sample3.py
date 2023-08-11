import csv
import dataclasses
import json
import argparse
import os
import random
import sys
import uuid
import time
import logging
from datetime import datetime, date, timedelta, timezone
from typing import List, TypeVar
from data_model import Product, Customer, Order, OrderLine
from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub import TransportType


DataClass = TypeVar('DataClass')

log = logging.getLogger("order-generator")
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))


def generate_json_data():
    # Replace this function with your logic to generate JSON data
    return {"order_id": "123-8eca-466f-872f-f1e2fd49e7e1",
            "customer_id": "0674efa6-ea2c-4317-8fa2-b685e15ce3b7",
            "order_lines": [{"product_id": "8e8b88f9-3df2-4241-a6a2-8ecabce05474", "volume": 90, "price": 97.3},
                            {"product_id": "690a2da3-241f-4cff-b8f2-af4f9b777ff5", "volume": 71, "price": 15.75}],
            "amount": 9875.25,
            "timestamp": "2023-08-05T12:00:58.567814"}


def load_csv_data(file_path: str, data_class: DataClass) -> List[DataClass]:
    result = []
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            result.append(data_class.from_dict(row))

    return result


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def write_json_data(file_path: str, data: List):
    with open(file_path, 'w') as jf:
        for item in data:
            json.dump(dataclasses.asdict(item), jf, default=json_serial)
            jf.write('\n')


def send_data_to_eventhub(producer, event_data_batch):
    try:
        with producer:
            producer.send_batch(event_data_batch)
        print("Data sent successfully.")
    except Exception as e:
        print(f"Error sending data: {str(e)}")


def generate_historical_data(start_date: datetime.date, end_date: datetime.date, max_orders_per_day: int,
                             customers: List[Customer], products: List[Product]) -> List[Order]:
    current_date = start_date
    orders = []
    while current_date <= end_date:
        orders_for_date = random.randint(
            int(max_orders_per_day / 2), max_orders_per_day)
        seconds_per_order = 86400 / orders_for_date
        for o in range(orders_for_date):
            customer_position = random.randint(0, len(customers) - 1)
            seconds_since_midnight = o * seconds_per_order
            timestamp = datetime.combine(current_date, datetime.min.time(), timezone.utc) + timedelta(
                seconds=seconds_since_midnight)
            order = generate_order(
                customers[customer_position], products, timestamp)
            orders.append(order)
        current_date = current_date + timedelta(days=1)

    return orders


def generate_order_lines(total_lines, products: List[Product]) -> List[OrderLine]:
    order_lines = []
    for _ in range(total_lines):
        product_index = random.randint(0, len(products) - 1)
        product = products[product_index]
        product_id = product.product_id
        price = product.price
        volume = random.randint(0, 100)
        order_line = OrderLine(product_id, volume, price)
        order_lines.append(order_line)

    return order_lines


def generate_order(customer: Customer, products: List[Product], timestamp: datetime) -> Order:
    order_id = str(uuid.uuid4())
    customer_id = customer.customer_id
    total_order_lines = random.randint(1, 100)
    order_lines = generate_order_lines(total_order_lines, products)
    amount = sum(ol.volume * ol.price for ol in order_lines)
    return Order(order_id=order_id, customer_id=customer_id, order_lines=order_lines, amount=amount,
                 timestamp=timestamp)


def order_stream(orders_per_minute: int, customers: List[Customer], products: List[Product]):
    avg_wait_time_s = 60 / orders_per_minute
    wait_time_delta = 0
    total_wait_time = 0
    total_orders_in_min = 0


    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://brenntag1.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=PZUjh4doT1l6fkcFmUUSa+qLl7S292nty+AEhAq3U+M=;EntityPath=brenntag_eh",
    eventhub_name="brenntag_eh", transport_type=TransportType.AmqpOverWebsocket)


    while True:
        # Create a new batch for each iteration
        event_data_batch = producer.create_batch()
        if total_orders_in_min == orders_per_minute or total_wait_time > 60:
            if wait_time_delta > 0 and total_wait_time < 60:
                time.sleep(wait_time_delta)
                total_wait_time = 0
            else:
                total_wait_time = wait_time_delta
            wait_time_delta = 0

        wait_time_s = random.random() * avg_wait_time_s * 2
        timestamp = datetime.utcnow()
        customer_position = random.randint(0, len(customers) - 1)

        order = generate_order(
            customers[customer_position], products, timestamp)
        log.info(f"Generated order: ${order.order_id}")
        # Generate JSON data

        # Generate JSON data
        json_data = json.dumps(
            dataclasses.asdict(order), default=json_serial)

        # Create an instance of EventData with the JSON data
        event_data = EventData(json_data)
        # print(type(event_data_batch))
        # print(dir(event_data_batch))
        # Add the EventData to the batch
        event_data_batch.add(event_data)
        # Send the batch of events to Azure Event Hub
        producer.send_batch(event_data_batch)

        # Wait for a specific interval before sending the next batch
        time.sleep(1)


def run(should_generate: bool, customers_path: str, products_path: str):
    customers = load_csv_data(customers_path, Customer)
    products = load_csv_data(products_path, Product)

    if should_generate:
        log.info("Generating historical data")
        orders = generate_historical_data(date(2022, 1, 1), date.today(), max_orders_per_day=200,
                                          customers=customers, products=products)
        orders_file = '/Users/mohammad.saif/Desktop/brenntag/interview-assignment-empty/data/orders.json'
        log.info(
            f"Generated {len(orders)} orders. Writing them to {orders_file}")
        write_json_data(orders_file, orders)
        log.info("Writing done.")
    else:
        log.info("Generating orders stream")
        order_stream(10, customers, products)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Order generator')
    parser.add_argument('--gen', dest='operation', required=False, action='store_true',
                        help='when selected, it will generate initial data set, otherwise it will stream data to a kafka topic.')
    parser.add_argument('--customers', dest='customers_path',
                        required=True, help='Path to csv file with customers')
    parser.add_argument('--products', dest='products_path',
                        required=True, help='Path to csv file with products')
    args = parser.parse_args()
    run(should_generate=False,
        customers_path=args.customers_path,
        products_path=args.products_path)
