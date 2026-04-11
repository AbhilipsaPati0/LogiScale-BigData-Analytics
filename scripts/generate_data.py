from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()

data = []

for i in range(5000):  # you can increase later (e.g., 100k)
    eta = random.randint(1, 10)
    delay = random.randint(-2, 5)
    
    record = {
        "RouteID": f"R{random.randint(1,50)}",
        "DriverID": f"D{random.randint(1,100)}",
        "Warehouse": f"W{random.randint(1,20)}",
        "ETA": eta,
        "ActualTime": eta + delay,
        "Distance_km": random.randint(5, 500),
        "Date": fake.date_between(start_date='-30d', end_date='today')
    }
    
    data.append(record)

df = pd.DataFrame(data)
df.to_csv("data/logistics_data.csv", index=False)

print("Dataset generated successfully!")