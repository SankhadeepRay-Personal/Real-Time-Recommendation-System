# data_generators/generate_products.py
import pandas as pd
import random
from faker import Faker

fake = Faker()
random.seed(24)

PRODUCT_CATALOG = {
    "Electronics": {
        "subcategories": ["Smartphones", "Laptops", "Headphones", "Smartwatches", "Cameras", "Tablets"],
        "brands": ["Samsung", "Apple", "Sony", "OnePlus", "Dell", "HP", "Canon", "Boat", "JBL"],
    },
    "Fashion": {
        "subcategories": ["Men's Clothing", "Women's Clothing", "Footwear", "Watches", "Bags"],
        "brands": ["Nike", "Adidas", "Zara", "H&M", "Puma", "Levis", "Fossil"],
    },
    "Home & Kitchen": {
        "subcategories": ["Cookware", "Furniture", "Home Decor", "Kitchen Appliances"],
        "brands": ["Prestige", "IKEA", "Philips", "Borosil", "Milton"],
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Comics", "Academic", "Children"],
        "brands": ["Penguin", "HarperCollins", "Scholastic", "Oxford"],
    },
    "Sports & Fitness": {
        "subcategories": ["Gym Equipment", "Outdoor Sports", "Yoga", "Cycling"],
        "brands": ["Decathlon", "Nike", "Adidas", "Cosco"],
    },
    "Beauty & Personal Care": {
        "subcategories": ["Skincare", "Haircare", "Makeup", "Fragrances"],
        "brands": ["L'Oreal", "Nivea", "Maybelline", "Dove"],
    },
    "Grocery": {
        "subcategories": ["Snacks", "Beverages", "Staples", "Dairy"],
        "brands": ["Nestle", "Amul", "Tata", "ITC"],
    },
    "Toys & Games": {
        "subcategories": ["Action Figures", "Board Games", "Puzzles", "Educational Toys"],
        "brands": ["Lego", "Hasbro", "Mattel", "Funskool"],
    }
}

def generate_products(start_id=5001):
    products = []
    product_id = start_id

    for category, meta in PRODUCT_CATALOG.items():
        for subcat in meta["subcategories"]:
            # generate 5-8 products per subcategory
            for _ in range(random.randint(5, 8)):
                brand = random.choice(meta["brands"])
                base_price = round(random.uniform(199, 89999), 2)
                discount = random.choice([0, 5, 10, 15, 20, 30, 40])
                final_price = round(base_price * (1 - discount / 100), 2)

                products.append({
                    "productId": product_id,
                    "productName": f"{brand} {subcat} {fake.word().capitalize()}-{random.randint(100,999)}",
                    "category": category,
                    "subCategory": subcat,
                    "brand": brand,
                    "basePrice": base_price,
                    "discountPercent": discount,
                    "finalPrice": final_price,
                    "rating": round(random.uniform(2.5, 5.0), 1),
                    "numReviews": random.randint(0, 5000),
                    "stockQuantity": random.randint(0, 1000),
                    "isActive": random.choices([1, 0], weights=[95, 5])[0]
                })
                product_id += 1
    return pd.DataFrame(products)

if __name__ == "__main__":
    df = generate_products()
    df.to_csv("C:\\Users\\sgiri055\\OneDrive - PwC\\POC\\Data_Engineering_Kafka\\products.csv", index=False)
    print(f"Generated {len(df)} products -> products.csv")
    print(df.head())