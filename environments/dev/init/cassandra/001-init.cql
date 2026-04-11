CREATE KEYSPACE IF NOT EXISTS review_keyspace
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE review_keyspace;

CREATE TABLE IF NOT EXISTS reviews (
    review_id text PRIMARY KEY,
    product_id text,
    customer_id text,
    star_rating int,
    review_body text,
    created_at timestamp
);