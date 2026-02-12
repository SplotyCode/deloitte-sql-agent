import sqlite3
import random
import os
from faker import Faker

def generate_sample_dump(filename="sample_dump.sql", db_filename="sample_dump.db"):
    if os.path.exists(db_filename):
        os.remove(db_filename)

    fake = Faker()
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    cursor.execute("PRAGMA foreign_keys = ON;")

    print("Creating tables...")
    cursor.execute("""
    CREATE TABLE categories (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        description TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        email TEXT NOT NULL UNIQUE,
        full_name TEXT,
        bio TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cursor.execute("""
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        category_id INTEGER,
        name TEXT NOT NULL,
        price REAL NOT NULL,
        description TEXT,
        stock_quantity INTEGER DEFAULT 0,
        FOREIGN KEY (category_id) REFERENCES categories (id)
    );
    """)

    cursor.execute("""
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT CHECK(status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
        total_amount REAL,
        FOREIGN KEY (user_id) REFERENCES users (id)
    );
    """)

    cursor.execute("""
    CREATE TABLE order_items (
        id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products (id)
    );
    """)

    cursor.execute("""
    CREATE TABLE reviews (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        rating INTEGER CHECK(rating BETWEEN 1 AND 5),
        comment TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (product_id) REFERENCES products (id)
    );
    """)

    print("Generating categories...")
    categories = ['Electronics', 'Books', 'Clothing', 'Home & Garden', 'Toys', 'Sports', 'Beauty', 'Automotive']
    category_ids = []
    for name in categories:
        cursor.execute("INSERT INTO categories (name, description) VALUES (?, ?)", 
                       (name, fake.sentence()))
        category_ids.append(cursor.lastrowid)

    print("Generating users...")
    user_ids = []
    for i in range(154_341):
        username = f"{fake.user_name()}_{i}"
        email = f"{i}_{fake.email()}"
        full_name = fake.name()
        bio = fake.paragraph()
        created_at = fake.date_time_this_year().isoformat()
        cursor.execute("INSERT INTO users (username, email, full_name, bio, created_at) VALUES (?, ?, ?, ?, ?)", 
                       (username, email, full_name, bio, created_at))
        user_ids.append(cursor.lastrowid)

    print("Generating products...")
    product_ids = []
    for _ in range(131):
        category_id = random.choice(category_ids)
        name = fake.catch_phrase()
        price = round(random.uniform(5.0, 1000.0), 2)
        description = fake.text()
        stock = random.randint(0, 100)
        cursor.execute("INSERT INTO products (category_id, name, price, description, stock_quantity) VALUES (?, ?, ?, ?, ?)", 
                       (category_id, name, price, description, stock))
        product_ids.append(cursor.lastrowid)

    print("Generating orders and items...")
    for _ in range(250_023):
        user_id = random.choice(user_ids)
        order_date = fake.date_time_this_year().isoformat()
        status = random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled'])
        
        cursor.execute("INSERT INTO orders (user_id, order_date, status) VALUES (?, ?, ?)", 
                       (user_id, order_date, status))
        order_id = cursor.lastrowid

        total_amount = 0
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 3)
            cursor.execute("SELECT price FROM products WHERE id = ?", (product_id,))
            unit_price = cursor.fetchone()[0]
            
            cursor.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)", 
                           (order_id, product_id, quantity, unit_price))
            total_amount += unit_price * quantity
            
        cursor.execute("UPDATE orders SET total_amount = ? WHERE id = ?", (total_amount, order_id))

    print("Generating reviews...")
    for _ in range(220_534):
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        rating = random.randint(1, 5)
        comment = fake.paragraph()
        created_at = fake.date_time_this_year().isoformat()
        cursor.execute("INSERT INTO reviews (user_id, product_id, rating, comment, created_at) VALUES (?, ?, ?, ?, ?)", 
                       (user_id, product_id, rating, comment, created_at))

    conn.commit()
    print("Database populated. Generating SQL dump...")

    with open(filename, 'w', encoding='utf-8') as f:
        for line in conn.iterdump():
            f.write('%s\n' % line)

    conn.close()
    print(f"Sample dump generated: {filename}")
    print(f"SQLite database generated: {db_filename}")

if __name__ == "__main__":
    generate_sample_dump()
