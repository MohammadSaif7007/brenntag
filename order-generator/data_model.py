from dataclasses import dataclass
from typing import List
from dateutil import parser
import datetime


@dataclass
class Customer:
    customer_id: str
    company_name: str
    specialized_industries: List[str]

    @staticmethod
    def from_dict(raw_dict) -> 'Customer':
        return Customer(
            customer_id=raw_dict['customer_id'],
            company_name=raw_dict['company_name'],
            specialized_industries=raw_dict['specialized_industries'].split(";")
        )


@dataclass
class Product:
    product_id: str
    product_name: str
    price: float

    @staticmethod
    def from_dict(raw_dict) -> 'Product':
        return Product(
            product_id=raw_dict['product_id'],
            product_name=raw_dict['product_name'],
            price=float(raw_dict['price'])
        )


@dataclass
class OrderLine:
    product_id: str
    volume: int
    price: float

    @staticmethod
    def from_dict(raw_dict) -> 'OrderLine':
        return OrderLine(
            product_id=raw_dict['product_id'],
            volume=int(raw_dict['volume']),
            price=float(raw_dict['price'])
        )


@dataclass
class Order:
    order_id: str
    customer_id: str
    order_lines: List[OrderLine]
    amount: float
    timestamp: datetime

    @staticmethod
    def from_dict(raw_dict) -> 'Order':
        return Order(
            order_id=raw_dict['order_id'],
            customer_id=raw_dict['customer_id'],
            order_lines=[OrderLine.from_dict(ol) for ol in raw_dict['order_lines']],
            amount=float(raw_dict['amount']),
            timestamp=parser.parse(raw_dict['timestamp'])
        )
