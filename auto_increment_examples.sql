-- SQL Engine with Auto-Increment Examples

-- Example 1: Table with auto-increment ID column
CREATE TABLE users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT);

-- Insert data without specifying ID (will auto-increment)
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');

-- Insert data with specific ID (will update auto-increment counter)
INSERT INTO users (id, name, email) VALUES (10, 'Bob Johnson', 'bob@example.com');

-- Next insert will continue from 11
INSERT INTO users (name, email) VALUES ('Alice Brown', 'alice@example.com');

-- Example 2: Table with LONG auto-increment column (for large sequences)
CREATE TABLE orders (order_id LONG AUTO_INCREMENT, customer_name TEXT, amount REAL);

-- Insert data without specifying order_id
INSERT INTO orders (customer_name, amount) VALUES ('John Doe', 25.99);
INSERT INTO orders (customer_name, amount) VALUES ('Jane Smith', 150.00);

-- Example 3: Table with multiple auto-increment columns (if needed)
CREATE TABLE products (id INTEGER AUTO_INCREMENT, serial_number LONG AUTO_INCREMENT, name TEXT, price REAL);

-- Insert data (both auto-increment columns will be assigned)
INSERT INTO products (name, price) VALUES ('Widget A', 19.99);
INSERT INTO products (name, price) VALUES ('Widget B', 29.99);

-- Example 4: Traditional table without auto-increment
CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT NOT NULL);

-- Must specify ID for non-auto-increment primary key
INSERT INTO categories VALUES (1, 'Electronics');
INSERT INTO categories VALUES (2, 'Books');

-- Query examples
SELECT * FROM users;
SELECT * FROM orders;
SELECT * FROM products;
SELECT * FROM categories;

SHOW TABLES;
