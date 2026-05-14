-- Проверка модели звезда в PostgreSQL
-- Подключение: localhost:5432, база lab2

-- Количество строк в каждой таблице
SELECT 'mock_data'    AS tbl, COUNT(*) FROM mock_data
UNION ALL
SELECT 'dim_customer',        COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_seller',          COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_product',         COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_store',           COUNT(*) FROM dim_store
UNION ALL
SELECT 'dim_supplier',        COUNT(*) FROM dim_supplier
UNION ALL
SELECT 'dim_date',            COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_sales',          COUNT(*) FROM fact_sales;

SELECT * FROM dim_product LIMIT 5;
SELECT * FROM dim_customer LIMIT 5;
SELECT * FROM fact_sales LIMIT 5;

-- Проверка связей
SELECT f.id, c.first_name, c.last_name, p.name, s.store_name, d.date, f.total_price
FROM fact_sales f
JOIN dim_customer c  ON f.customer_id = c.id
JOIN dim_product  p  ON f.product_id  = p.id
JOIN dim_store    s  ON f.store_id    = s.id
JOIN dim_date     d  ON f.date_id     = d.id
LIMIT 10;
