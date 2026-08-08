# data_generators/generate_customers.py
import pandas as pd
import random
from faker import Faker

fake = Faker()
random.seed(42)

CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Books",
              "Sports & Fitness", "Beauty & Personal Care", "Grocery", "Toys & Games"]

SEGMENTS = ["New", "Regular", "Premium", "VIP"]
GENDERS = ["Male", "Female", "Other"]
DEVICE_PREF = ["Mobile", "Desktop", "Tablet"]

CITIES = [
    ("Mumbai", "Maharashtra", "India"), ("Delhi", "Delhi", "India"),
    ("Bengaluru", "Karnataka", "India"), ("Hyderabad", "Telangana", "India"),
    ("Chennai", "Tamil Nadu", "India"), ("Pune", "Maharashtra", "India"),
    ("New York", "New York", "USA"), ("Chicago", "Illinois", "USA"),
    ("London", "England", "UK"), ("Toronto", "Ontario", "Canada")
]

def generate_customers(n=550, start_id=101):
    customers = []
    for i in range(n):
        customer_id = start_id + i
        city, state, country = random.choice(CITIES)
        signup_date = fake.date_between(start_date="-3y", end_date="-1d")

        customers.append({
            "customerId": customer_id,
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "age": random.randint(18, 65),
            "gender": random.choices(GENDERS, weights=[45, 45, 10])[0],
            "city": city,
            "state": state,
            "country": country,
            "registrationDate": signup_date.strftime("%Y-%m-%d"),
            "customerSegment": random.choices(SEGMENTS, weights=[30, 40, 20, 10])[0],
            "preferredCategory": random.choice(CATEGORIES),
            "preferredDevice": random.choice(DEVICE_PREF),
            "loyaltyPoints": random.randint(0, 5000),
            "isActive": random.choices([1, 0], weights=[90, 10])[0]
        })
    return pd.DataFrame(customers)

if __name__ == "__main__":
    df = generate_customers()
    #df.to_csv("../customers.csv", index=False) #C:\Users\smondal234\OneDrive - PwC\Data_Engineering_Kafka\data_generator
    df.to_csv("C:\\Users\\sgiri055\\OneDrive - PwC\\POC\\Data_Engineering_Kafka\\customers.csv", index=False)
    print(f"Generated {len(df)} customers -> customers.csv")
    print(df.head())