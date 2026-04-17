from kafka_producer.producer import create_transaction


def test_transaction_message_structure():
    message = create_transaction()

    required_keys = [
        "transaction_id",
        "customer_id",
        "product_id",
        "amount",
        "city",
        "payment_method",
        "event_time",
        "status"
    ]

    for key in required_keys:
        assert key in message


def test_transaction_amount_positive():
    message = create_transaction()

    assert message["amount"] > 0


def test_payment_method_valid():
    message = create_transaction()

    valid_methods = ["UPI", "Card", "NetBanking"]
    assert message["payment_method"] in valid_methods