CREATE KEYSPACE IF NOT EXISTS amazon_reviews 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon_reviews;

-- 1 & 2: Пошук по product_id та star_rating
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id text,
    star_rating int,
    review_id text,
    customer_id text,
    review_body text,
    review_date date,
    PRIMARY KEY (product_id, star_rating, review_id)
) WITH CLUSTERING ORDER BY (star_rating DESC, review_id ASC);

-- 3: Пошук по customer_id
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id text,
    review_date date,
    review_id text,
    product_id text,
    star_rating int,
    verified_purchase text,
    PRIMARY KEY (customer_id, review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 4, 5, 6, 7: Для аналітики за період (використовуємо рік-місяць як роздільник)
CREATE TABLE IF NOT EXISTS reviews_for_analytics (
    year_month text,
    review_date date,
    customer_id text,
    product_id text,
    star_rating int,
    verified_purchase text,
    review_id text,
    PRIMARY KEY (year_month, review_date, review_id)
);