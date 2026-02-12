import sqlite3
import random
import os

def generate_sample_dump(filename="sample_dump.sql", db_filename="sample_dump.db"):
    if os.path.exists(db_filename):
        os.remove(db_filename)

    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        bio TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price REAL NOT NULL,
        description TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        order_date TEXT,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (product_id) REFERENCES products (id)
    );
    """)
    users = []
    for i in range(1, 10001):
        username = f"user_{i}"
        email = f"user_{i}@example.com"
        bio = f"This is the bio for user {i}. It might contain some interesting or uninteresting info."
        cursor.execute("INSERT INTO users (id, username, email, bio) VALUES (?, ?, ?, ?)", (i, username, email, bio))
        users.append(i)
    products = []
    for i in range(1, 21):
        name = f"Product {i}"
        price = round(random.uniform(10.0, 500.0), 2)
        description = f"Description for product {i}. Features include X, Y, and Z."
        cursor.execute("INSERT INTO products (id, name, price, description) VALUES (?, ?, ?, ?)", (i, name, price, description))
        products.append(i)
    for i in range(1, 50001):
        user_id = random.choice(users)
        product_id = random.choice(products)
        quantity = random.randint(1, 5)
        order_date = f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        cursor.execute("INSERT INTO orders (id, user_id, product_id, quantity, order_date) VALUES (?, ?, ?, ?, ?)", 
                       (i, user_id, product_id, quantity, order_date))

    conn.commit()

    with open(filename, 'w') as f:
        for line in conn.iterdump():
            f.write('%s\n' % line)

    conn.close()
    print(f"Sample dump generated: {filename}")
    print(f"SQLite database generated: {db_filename}")

if __name__ == "__main__":
    generate_sample_dump()
