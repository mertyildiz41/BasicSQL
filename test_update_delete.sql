-- Test UPDATE and DELETE operations
CREATE TABLE test_users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INTEGER);

-- Insert some test data
INSERT INTO test_users (name, email, age) VALUES ('Alice', 'alice@example.com', 25);
INSERT INTO test_users (name, email, age) VALUES ('Bob', 'bob@example.com', 30);
INSERT INTO test_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35);

-- Show initial data
SELECT * FROM test_users;

-- Test UPDATE operation
UPDATE test_users SET age = 26 WHERE name = 'Alice';

-- Show data after update
SELECT * FROM test_users;

-- Test DELETE operation
DELETE FROM test_users WHERE name = 'Bob';

-- Show final data
SELECT * FROM test_users;

-- Exit
.quit
