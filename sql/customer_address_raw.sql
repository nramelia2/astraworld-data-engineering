CREATE TABLE customer_addresses_raw(
    id INT,
    customer_id INT,
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    created_at DATETIME
);