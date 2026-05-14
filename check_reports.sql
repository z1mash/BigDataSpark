-- Проверка 6 витрин данных в ClickHouse
-- Подключение: localhost:8123, база default, user=default, password=пусто

SHOW TABLES;

SELECT product_id, name, category, total_quantity, total_revenue
FROM dm_product_sales
ORDER BY total_quantity DESC
LIMIT 10;

-- Выручка по категориям
SELECT category, SUM(total_revenue) AS revenue
FROM dm_product_sales
GROUP BY category
ORDER BY revenue DESC;

-- Средний рейтинг и отзывы
SELECT name, category, rating, reviews
FROM dm_product_sales
ORDER BY rating DESC
LIMIT 10;

-- 2. Витрина продаж по клиентам
-- Топ-10 клиентов
SELECT customer_id, first_name, last_name, country, total_spent
FROM dm_customer_sales
ORDER BY total_spent DESC
LIMIT 10;

-- Распределение по странам
SELECT country, COUNT(*) AS customer_count, SUM(total_spent) AS total_revenue
FROM dm_customer_sales
GROUP BY country
ORDER BY total_revenue DESC;

-- Средний чек
SELECT customer_id, first_name, last_name, avg_check
FROM dm_customer_sales
ORDER BY avg_check DESC
LIMIT 10;

-- 3. Витрина продаж по времени
-- Месячные тренды
SELECT year, month, monthly_revenue, avg_order_size, order_count
FROM dm_time_sales
ORDER BY year, month;

-- Годовые тренды
SELECT year, SUM(monthly_revenue) AS yearly_revenue
FROM dm_time_sales
GROUP BY year
ORDER BY year;

-- 4. Витрина продаж по магазинам
-- Топ-5 магазинов
SELECT store_name, city, country, total_revenue, avg_check
FROM dm_store_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение по городам
SELECT city, country, SUM(total_revenue) AS total_revenue
FROM dm_store_sales
GROUP BY city, country
ORDER BY total_revenue DESC
LIMIT 10;

-- 5. Витрина продаж по поставщикам
-- Топ-5 поставщиков
SELECT supplier_name, supplier_country, total_revenue, avg_product_price
FROM dm_supplier_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение по странам
SELECT supplier_country, SUM(total_revenue) AS total_revenue
FROM dm_supplier_sales
GROUP BY supplier_country
ORDER BY total_revenue DESC;

-- 6. Витрина качества продукции
-- Наивысший рейтинг
SELECT name, category, rating, reviews, total_sold
FROM dm_product_quality
ORDER BY rating DESC
LIMIT 10;

-- Наименьший рейтинг
SELECT name, category, rating, reviews, total_sold
FROM dm_product_quality
ORDER BY rating ASC
LIMIT 10;

-- Корреляция рейтинга и продаж
SELECT name, rating, total_sold
FROM dm_product_quality
ORDER BY total_sold DESC
LIMIT 10;

-- Наибольшее количество отзывов
SELECT name, category, reviews, rating
FROM dm_product_quality
ORDER BY reviews DESC
LIMIT 10;
