import json
import time
import random
import pandas as pd
from faker import Faker
from IPython.display import display, clear_output
import ConnectBigQuery as BQ
from google.cloud import bigquery

# Initialize Faker
fake = Faker()

# Define a variety of event types to simulate real customer behavior
EVENT_TYPES = [
    "page_view", "search_product", "view_product", "add_to_cart",
    "remove_from_cart", "wishlist_add", "wishlist_remove", "apply_coupon",
    "checkout", "purchase", "login", "logout"
]

# Create an empty DataFrame
columns = ["event_id", "user_id", "event_type", "product_id", "price", "timestamp"]
events_df = pd.DataFrame(columns=columns)

# Function to generate a fake event
def generate_event():
    event_type = random.choice(EVENT_TYPES)  # Randomly pick an event type
    
    # Some events might not have a product_id or price (e.g., login, logout)
    product_id = fake.uuid4() if event_type not in ["login", "logout"] else None
    price = round(random.uniform(5, 500), 2) if event_type in ["add_to_cart", "purchase", "apply_coupon", "checkout"] else None
    
    return {
        "event_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "event_type": event_type,
        "product_id": product_id,
        "price": price,
        "timestamp": pd.Timestamp.now().isoformat()
    }

def stream_to_bigquery(event):
    table_id = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.events"
    client = bigquery.Client()
    client.query(R"SELECT * FROM {table_id}")
    # Insert event into BigQuery
    errors = client.insert_rows_json(table_id, [event])
    
    if errors:
        print(f"Error inserting event: {errors}")
    else:
        print(f"Inserted event: {event}")
        


# Simulate real-time events
for _ in range(100):  # Generate 20 events in real time
    new_event = generate_event()  # Generate a new event
    stream_to_bigquery(new_event)
    events_df = pd.concat([events_df, pd.DataFrame([new_event])], ignore_index=True)  # Append to DataFrame
    
    # Clear previous output and display updated DataFrame
    clear_output(wait=True)
    # display(events_df)
    
    time.sleep(1)  # Simulate real-time event streaming (1-second delay)
