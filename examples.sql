-- Example SQL scripts for the Simple SQL Engine (C#)
-- Run with: dotnet run --file examples.sql

-- Create a users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER,
    email TEXT
);

-- Insert some sample data
INSERT INTO users VALUES (1, 'John Doe', 30, 'john@example.com');
INSERT INTO users VALUES (2, 'Jane Smith', 25, 'jane@example.com');
INSERT INTO users VALUES (3, 'Bob Johnson', 35, 'bob@example.com');
INSERT INTO users VALUES (4, 'Alice Brown', 28, 'alice@example.com');
INSERT INTO users VALUES (5, 'Charlie Wilson', 32, 'charlie@example.com');

-- Create a products table
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL,
    category TEXT
);

-- Insert product data
INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics');
INSERT INTO products VALUES (2, 'Coffee Mug', 12.50, 'Home');
INSERT INTO products VALUES (3, 'Smartphone', 699.99, 'Electronics');
INSERT INTO products VALUES (4, 'Desk Chair', 199.99, 'Furniture');
INSERT INTO products VALUES (5, 'Book', 24.99, 'Education');

-- Show all tables
SHOW TABLES;

-- Select all users
SELECT * FROM users;

-- Select specific columns
SELECT name, email FROM users;

-- Filter with WHERE clause
SELECT * FROM users WHERE age > 25;

-- Order results
SELECT * FROM users ORDER BY age DESC;

-- Select with limit
SELECT * FROM users ORDER BY name LIMIT 3;

-- Update a record
UPDATE users SET age = 31 WHERE name = 'John Doe';

-- Verify the update
SELECT * FROM users WHERE name = 'John Doe';

-- Select from products
SELECT * FROM products;

-- Filter products by category
SELECT name, price FROM products WHERE category = 'Electronics';

-- Find expensive products
SELECT * FROM products WHERE price > 100 ORDER BY price DESC;

-- Update product price
UPDATE products SET price = 899.99 WHERE name = 'Laptop';

-- Delete a user
DELETE FROM users WHERE age < 26;

-- Show remaining users
SELECT * FROM users;

-- Delete products in a category
DELETE FROM products WHERE category = 'Home';

-- Show remaining products
SELECT * FROM products;
