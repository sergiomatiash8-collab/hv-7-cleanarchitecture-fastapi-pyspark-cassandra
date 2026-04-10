-- Create a Keyspace (analogous to a Database in SQL)
-- We use SimpleStrategy for local development with one node
CREATE KEYSPACE IF NOT EXISTS reviews_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reviews_keyspace;

-- Table 1: Optimized for fetching reviews by a specific product
-- Primary Key: ((product_id) is Partition Key, others are Clustering Keys for sorting)
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id text,
    created_at timestamp,
    review_id text,
    customer_id text,
    star_rating int,
    review_body text,
    PRIMARY KEY ((product_id), created_at, review_id)
) WITH CLUSTERING ORDER BY (created_at DESC, review_id ASC);

-- Table 2: Optimized for fetching reviews by product AND a specific rating [cite: 65]
-- This avoids "ALLOW FILTERING" by including star_rating in the Partition Key
CREATE TABLE IF NOT EXISTS reviews_by_product_rating (
    product_id text,
    star_rating int,
    created_at timestamp,
    review_id text,
    customer_id text,
    review_body text,
    PRIMARY KEY ((product_id, star_rating), created_at, review_id)
) WITH CLUSTERING ORDER BY (created_at DESC, review_id ASC);

-- Table 3: Optimized for fetching all reviews written by a specific customer [cite: 66]
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id text,
    created_at timestamp,
    review_id text,
    product_id text,
    star_rating int,
    review_body text,
    PRIMARY KEY ((customer_id), created_at, review_id)
) WITH CLUSTERING ORDER BY (created_at DESC, review_id ASC);