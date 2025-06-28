-- Test auto-increment functionality
CREATE TABLE users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT);

-- Insert data without specifying ID
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');

-- Insert data with specific ID
INSERT INTO users (id, name, email) VALUES (10, 'Bob Johnson', 'bob@example.com');

-- Next insert should continue from 11
INSERT INTO users (name, email) VALUES ('Alice Brown', 'alice@example.com');

-- Check the results
SELECT * FROM users;

-- Show tables
.tables

-- Exit
.quit
