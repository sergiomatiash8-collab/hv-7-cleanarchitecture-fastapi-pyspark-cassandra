CREATE KEYSPACE IF NOT EXISTS amazon_reviews 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon_reviews;

-- 1 & 2: Пошук по product_id та star_rating
-- Додано star_rating у PK для швидкої фільтрації
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

-- Загальна таблиця для сирих даних (залишаємо твою структуру)
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

-- 4: Топ N найпопулярніших товарів за період
CREATE TABLE IF NOT EXISTS product_stats_by_period (
    period text,
    review_count int,
    product_id text,
    PRIMARY KEY (period, review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC);

-- 5: Топ N продуктивних покупців (verified_purchase = 'Y')
CREATE TABLE IF NOT EXISTS customer_verified_stats_by_period (
    period text,
    review_count int,
    customer_id text,
    PRIMARY KEY (period, review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC);

-- 6: Топ N "хейтерів" (1-2 зірки)
CREATE TABLE IF NOT EXISTS hater_stats_by_period (
    period text,
    bad_reviews_count int,
    customer_id text,
    PRIMARY KEY (period, bad_reviews_count, customer_id)
) WITH CLUSTERING ORDER BY (bad_reviews_count DESC);

-- 7: Топ N "бекерів" (4-5 зірок)
CREATE TABLE IF NOT EXISTS backer_stats_by_period (
    period text,
    good_reviews_count int,
    customer_id text,
    PRIMARY KEY (period, good_reviews_count, customer_id)
) WITH CLUSTERING ORDER BY (good_reviews_count DESC);