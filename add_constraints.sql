ALTER TABLE dim_customer  ADD PRIMARY KEY (id);
ALTER TABLE dim_seller    ADD PRIMARY KEY (id);
ALTER TABLE dim_product   ADD PRIMARY KEY (id);
ALTER TABLE dim_store     ADD PRIMARY KEY (id);
ALTER TABLE dim_supplier  ADD PRIMARY KEY (id);
ALTER TABLE dim_date      ADD PRIMARY KEY (id);
ALTER TABLE fact_sales    ADD PRIMARY KEY (id);

ALTER TABLE fact_sales ADD CONSTRAINT fk_customer
    FOREIGN KEY (customer_id) REFERENCES dim_customer(id);

ALTER TABLE fact_sales ADD CONSTRAINT fk_seller
    FOREIGN KEY (seller_id) REFERENCES dim_seller(id);

ALTER TABLE fact_sales ADD CONSTRAINT fk_product
    FOREIGN KEY (product_id) REFERENCES dim_product(id);

ALTER TABLE fact_sales ADD CONSTRAINT fk_store
    FOREIGN KEY (store_id) REFERENCES dim_store(id);

ALTER TABLE fact_sales ADD CONSTRAINT fk_supplier
    FOREIGN KEY (supplier_id) REFERENCES dim_supplier(id);

ALTER TABLE fact_sales ADD CONSTRAINT fk_date
    FOREIGN KEY (date_id) REFERENCES dim_date(id);
