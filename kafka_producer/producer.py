from confluent_kafka import Producer
import json
import time
import random
import uuid
from datetime import datetime

from kafka_producer.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_USERNAME,
    KAFKA_PASSWORD,
    KAFKA_TOPIC,
    EVENTS_PER_BATCH,
    SLEEP_SECONDS
)

from utils.logger import get_logger

logger = get_logger("kafka_producer", "Producer.log")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(
            f"Delivered to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def run_producer(total_batches=5):
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISM,
        "sasl.username": KAFKA_USERNAME,
        "sasl.password": KAFKA_PASSWORD
    }

    producer = Producer(conf)

    products = [f"product_{i}" for i in range(1, 1000001)]

    city_state_map = {
        "Chennai": ("Tamil Nadu", "India", "600001"),
        "Coimbatore": ("Tamil Nadu", "India", "641001"),
        "Bangalore": ("Karnataka", "India", "560001"),
        "Mumbai": ("Maharashtra", "India", "400001"),
        "Pune": ("Maharashtra", "India", "411001"),
        "Nagpur": ("Maharashtra", "India", "440001"),
        "Delhi": ("Delhi", "India", "110001"),
        "Noida": ("Uttar Pradesh", "India", "201301"),
        "Ghaziabad": ("Uttar Pradesh", "India", "201001"),
        "Lucknow": ("Uttar Pradesh", "India", "226001"),
        "Kanpur": ("Uttar Pradesh", "India", "208001"),
        "Hyderabad": ("Telangana", "India", "500001"),
        "Kolkata": ("West Bengal", "India", "700001"),
        "Ahmedabad": ("Gujarat", "India", "380001"),
        "Surat": ("Gujarat", "India", "395003"),
        "Vadodara": ("Gujarat", "India", "390001"),
        "Jaipur": ("Rajasthan", "India", "302001"),
        "Patna": ("Bihar", "India", "800001"),
        "Bhopal": ("Madhya Pradesh", "India", "462001"),
        "Indore": ("Madhya Pradesh", "India", "452001"),
        "Ludhiana": ("Punjab", "India", "141001"),
        "Thiruvananthapuram": ("Kerala", "India", "695001"),
        "Agra": ("Uttar Pradesh", "India", "282001"),
        "Faridabad": ("Haryana", "India", "121001"),
        "Bhubaneswar": ("Odisha", "India", "751001"),
        "Rajkot": ("Gujarat", "India", "360001")
    }

    cities = list(city_state_map.keys())

    payment_methods = [
        "credit_card", "debit_card", "upi", "cash", "paytm", "googlepay",
        "phonepe", "amazonpay", "applepay", "paypal", "stripe"
    ]

    device_types = [
        "mobile", "tablet", "laptop", "desktop", "smartwatch", "other"
    ]

    currencies = {
        "INR": 1.0,
        "USD": 83.10,
        "EUR": 90.25,
        "GBP": 105.40,
        "AED": 22.63,
        "SGD": 61.45,
        "JPY": 0.56,
        "AUD": 54.10,
        "CAD": 60.75
    }

    customer_segments = ["NEW", "RETURNING", "VIP", "PREMIUM"]

    transaction_counter = 1

    logger.info(f"Kafka Producer started for {total_batches} batches")

    try:
        for batch_no in range(1, total_batches + 1):
            batch_count = 0

            for _ in range(EVENTS_PER_BATCH):
                event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                quantity = random.randint(1, 10)
                unit_price = round(random.uniform(500.0, 50000.0), 2)

                cost_price = round(unit_price * random.uniform(0.55, 0.90), 2)
                gross_amount = round(quantity * unit_price, 2)
                discount_amount = round(gross_amount * random.uniform(0.00, 0.20), 2)
                taxable_amount = gross_amount - discount_amount
                tax_amount = round(taxable_amount * 0.18, 2)
                net_amount = round(taxable_amount + tax_amount, 2)
                profit_amount = round((unit_price - cost_price) * quantity, 2)

                currency = random.choice(list(currencies.keys()))
                exchange_rate = currencies[currency]

                city = random.choice(cities)
                state, country, zip_code = city_state_map[city]

                customer_segment = random.choice(customer_segments)
                is_first_purchase = "YES" if customer_segment == "NEW" else random.choice(["YES", "NO"])

                transaction = {
                    "event_id": str(uuid.uuid4()),
                    "transaction_id": f"TXN{transaction_counter}",
                    "customer_id": random.randint(1, 100000),
                    "merchant_id": random.randint(1, 5000),
                    "product_id": random.randint(1, 1000),
                    "product_name": random.choice(products),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "gross_amount": gross_amount,
                    "discount_amount": discount_amount,
                    "tax_amount": tax_amount,
                    "net_amount": net_amount,
                    "payment_method": random.choice(payment_methods),
                    "city": city,
                    "device_type": random.choice(device_types),
                    "event_type": "transaction_created",
                    "transaction_status": "PENDING",
                    "event_time": event_time,
                    "ingestion_time": ingestion_time,
                    "currency": currency,
                    "exchange_rate": exchange_rate,
                    "profit_amount": profit_amount,
                    "cost_price": cost_price,
                    "order_status": "CREATED",
                    "payment_status": "PENDING",
                    "delivery_status": "NOT_SHIPPED",
                    "refund_amount": 0.0,
                    "customer_segment": customer_segment,
                    "is_first_purchase": is_first_purchase,
                    "session_id": str(uuid.uuid4()),
                    "state": state,
                    "country": country,
                    "zip_code": zip_code
                }

                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=json.dumps(transaction).encode("utf-8"),
                    callback=delivery_report
                )

                producer.poll(0)
                transaction_counter += 1
                batch_count += 1

            producer.flush()
            logger.info(f"Batch {batch_no}/{total_batches} completed with {batch_count} events")
            time.sleep(SLEEP_SECONDS)

        logger.info("Kafka Producer finished successfully")

    except Exception as e:
        logger.exception(f"Error while sending Kafka events: {e}")
        raise


def main():
    run_producer(total_batches=5)


if __name__ == "__main__":
    main()