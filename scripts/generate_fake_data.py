#!/usr/bin/env python3

"""Generate fake ecommerce data for RDS PostgreSQL."""

import csv
import os
import random
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from faker import Faker

faker = Faker()

CATEGORY_NAMES = [
    'Electronics',
    'Home & Kitchen',
    'Books',
    'Sports & Outdoors',
    'Beauty',
    'Toys & Games',
    'Fashion',
    'Health',
    'Automotive',
    'Garden'
]

PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'gift_card']
ORDER_STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']
SHIPPING_EVENTS = ['label_created', 'picked_up', 'in_transit', 'out_for_delivery', 'delivered', 'delivery_failed', 'returned']
INVENTORY_EVENTS = ['received', 'sold', 'adjusted', 'returned']
CART_EVENTS = ['added', 'removed', 'updated', 'abandoned']

OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'output'


def sanitize_run_suffix(run_suffix: Optional[str]) -> str:
    if not run_suffix:
        return ''
    sanitized = re.sub(r'[^a-zA-Z0-9]+', '_', run_suffix).strip('_').lower()
    return f"_{sanitized}" if sanitized else ''


def make_categories() -> List[Dict]:
    categories = []
    for name in CATEGORY_NAMES:
        categories.append({
            'name': name,
            'description': faker.sentence(nb_words=12),
            'created_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'updated_at': faker.date_time_between(start_date='-2y', end_date='now'),
        })
    return categories


def make_products(categories: List[Dict], count: int = 120, run_suffix: Optional[str] = None) -> List[Dict]:
    products = []
    suffix = sanitize_run_suffix(run_suffix)
    for idx in range(count):
        category = random.choice(categories)
        quantity = random.randint(0, 500)
        price = round(random.uniform(5.0, 499.99), 2)
        products.append({
            'category_id': categories.index(category) + 1,
            'name': faker.catch_phrase(),
            'description': faker.text(max_nb_chars=160),
            'price': price,
            'sku': f"SKU-{idx + 1:08d}{suffix}".upper(),
            'inventory_quantity': quantity,
            'created_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'updated_at': faker.date_time_between(start_date='-2y', end_date='now'),
        })
    return products


def make_users(count: int = 200, run_suffix: Optional[str] = None) -> List[Dict]:
    users = []
    suffix = sanitize_run_suffix(run_suffix)
    for idx in range(count):
        profile = faker.simple_profile()
        users.append({
            'first_name': profile['name'].split()[0],
            'last_name': ' '.join(profile['name'].split()[1:]) if len(profile['name'].split()) > 1 else faker.last_name(),
            'email': f"user_{idx + 1:08d}{suffix}@example.com",
            'phone': faker.phone_number(),
            'address': faker.street_address(),
            'city': faker.city(),
            'state': faker.state(),
            'country': faker.country(),
            'postal_code': faker.postcode(),
            'created_at': faker.date_time_between(start_date='-3y', end_date='now'),
            'updated_at': faker.date_time_between(start_date='-3y', end_date='now'),
        })
    return users


def make_orders(users: List[Dict], count: int = 300) -> List[Dict]:
    orders = []
    for _ in range(count):
        user_id = random.randint(1, len(users))
        order_date = faker.date_time_between(start_date='-1y', end_date='now')
        status = random.choices(ORDER_STATUSES, weights=[10, 25, 20, 35, 5, 5], k=1)[0]
        subtotal = round(random.uniform(20.0, 1200.0), 2)
        tax = round(subtotal * random.uniform(0.05, 0.12), 2)
        shipping_cost = round(random.uniform(0.0, 35.0), 2)
        total = round(subtotal + tax + shipping_cost, 2)
        orders.append({
            'user_id': user_id,
            'order_date': order_date,
            'status': status,
            'currency': 'USD',
            'subtotal': subtotal,
            'tax': tax,
            'shipping_cost': shipping_cost,
            'total_amount': total,
            'shipping_address': faker.address().replace('\n', ', '),
            'billing_address': faker.address().replace('\n', ', '),
            'created_at': order_date,
            'updated_at': faker.date_time_between(start_date=order_date, end_date='now'),
        })
    return orders


def make_order_items(orders: List[Dict], products: List[Dict], max_items_per_order: int = 5) -> List[Dict]:
    order_items = []
    for order_id in range(1, len(orders) + 1):
        line_count = random.randint(1, max_items_per_order)
        available_products = random.sample(products, k=line_count)
        for product in available_products:
            quantity = random.randint(1, 4)
            total_price = round(quantity * product['price'], 2)
            order_items.append({
                'order_id': order_id,
                'product_id': products.index(product) + 1,
                'quantity': quantity,
                'unit_price': product['price'],
                'total_price': total_price,
                'created_at': faker.date_time_between(start_date=orders[order_id - 1]['order_date'], end_date='now'),
                'updated_at': faker.date_time_between(start_date=orders[order_id - 1]['order_date'], end_date='now'),
            })
    return order_items


def make_payments(orders: List[Dict]) -> List[Dict]:
    payments = []
    for order_id, order in enumerate(orders, start=1):
        status = random.choices(['completed', 'pending', 'failed', 'refunded'], weights=[80, 10, 7, 3], k=1)[0]
        payments.append({
            'order_id': order_id,
            'payment_method': random.choice(PAYMENT_METHODS),
            'payment_status': status,
            'amount': order['total_amount'],
            'currency': 'USD',
            'transaction_id': faker.uuid4(),
            'processed_at': faker.date_time_between(start_date=order['order_date'], end_date='now'),
            'created_at': faker.date_time_between(start_date=order['order_date'], end_date='now'),
            'updated_at': faker.date_time_between(start_date=order['order_date'], end_date='now'),
        })
    return payments


def make_shipping_events(orders: List[Dict]) -> List[Dict]:
    shipping_events = []
    for order_id, order in enumerate(orders, start=1):
        event_count = random.randint(1, 4)
        event_base = order['order_date']
        for _ in range(event_count):
            event_base += timedelta(hours=random.randint(6, 48))
            shipping_events.append({
                'order_id': order_id,
                'event_type': random.choice(SHIPPING_EVENTS),
                'event_timestamp': event_base,
                'carrier': random.choice(['UPS', 'FedEx', 'DHL', 'USPS', 'Amazon Logistics']),
                'tracking_number': faker.bothify(text='TRACK-########'),
                'location': f"{faker.city()}, {faker.state_abbr()}",
                'created_at': event_base,
                'updated_at': event_base,
            })
    return shipping_events


def make_product_reviews(users: List[Dict], products: List[Dict], count: int = 180) -> List[Dict]:
    reviews = []
    for _ in range(count):
        user_id = random.randint(1, len(users))
        product_id = random.randint(1, len(products))
        review_date = faker.date_time_between(start_date='-1y', end_date='now')
        rating = random.randint(1, 5)
        reviews.append({
            'product_id': product_id,
            'user_id': user_id,
            'rating': rating,
            'title': faker.sentence(nb_words=6),
            'body': faker.paragraph(nb_sentences=3),
            'review_date': review_date,
            'created_at': review_date,
            'updated_at': review_date,
        })
    return reviews


def make_inventory_logs(products: List[Dict], count: int = 150) -> List[Dict]:
    logs = []
    for _ in range(count):
        product_id = random.randint(1, len(products))
        previous = products[product_id - 1]['inventory_quantity']
        event_type = random.choice(INVENTORY_EVENTS)
        delta = random.randint(1, 25)
        new_quantity = max(previous + delta if event_type in ('received', 'returned') else previous - delta, 0)
        logs.append({
            'product_id': product_id,
            'event_type': event_type,
            'quantity': delta,
            'previous_quantity': previous,
            'new_quantity': new_quantity,
            'event_timestamp': faker.date_time_between(start_date='-365d', end_date='now'),
            'notes': faker.sentence(nb_words=10),
            'created_at': faker.date_time_between(start_date='-365d', end_date='now'),
            'updated_at': faker.date_time_between(start_date='-365d', end_date='now'),
        })
    return logs


def make_cart_events(users: List[Dict], products: List[Dict], count: int = 240) -> List[Dict]:
    events = []
    for _ in range(count):
        events.append({
            'user_id': random.randint(1, len(users)),
            'product_id': random.randint(1, len(products)),
            'event_type': random.choice(CART_EVENTS),
            'quantity': random.randint(0, 3),
            'event_timestamp': faker.date_time_between(start_date='-90d', end_date='now'),
            'session_id': faker.uuid4(),
            'created_at': faker.date_time_between(start_date='-90d', end_date='now'),
            'updated_at': faker.date_time_between(start_date='-90d', end_date='now'),
        })
    return events


def save_csv(table_name: str, rows: List[Dict]):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR / f"{table_name}.csv"
    if not rows:
        return
    with file_path.open('w', newline='', encoding='utf-8') as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def assert_unique(rows: List[Dict], column: str, label: str):
    values = [row[column] for row in rows]
    duplicates = [value for value, count in Counter(values).items() if count > 1]

    if duplicates:
        raise ValueError(
            f"Duplicate {label} detected in generated data: {duplicates[:5]}"
        )


def generate_data(
    users_count: int = 200,
    product_count: int = 120,
    orders_count: int = 300,
    review_count: int = 180,
    inventory_log_count: int = 150,
    cart_event_count: int = 240,
    run_suffix: Optional[str] = None,
) -> Dict[str, List[Dict]]:
    categories = make_categories()
    products = make_products(categories, count=product_count, run_suffix=run_suffix)
    users = make_users(count=users_count, run_suffix=run_suffix)
    orders = make_orders(users, count=orders_count)
    order_items = make_order_items(orders, products)
    payments = make_payments(orders)
    shipping_events = make_shipping_events(orders)
    product_reviews = make_product_reviews(users, products, count=review_count)
    inventory_logs = make_inventory_logs(products, count=inventory_log_count)
    cart_events = make_cart_events(users, products, count=cart_event_count)

    assert_unique(users, 'email', 'user emails')
    assert_unique(products, 'sku', 'product SKUs')

    return {
        'categories': categories,
        'products': products,
        'users': users,
        'orders': orders,
        'order_items': order_items,
        'payments': payments,
        'shipping_events': shipping_events,
        'product_reviews': product_reviews,
        'inventory_logs': inventory_logs,
        'cart_events': cart_events,
    }


def main():
    data = generate_data()
    for table_name, rows in data.items():
        save_csv(table_name, rows)
        print(f"Wrote {len(rows)} rows to output/{table_name}.csv")

    print('\nFake ecommerce dataset generated successfully.')


if __name__ == '__main__':
    main()
