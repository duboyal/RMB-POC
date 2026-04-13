CREATE TABLE IF NOT EXISTS order_header (
    id SERIAL PRIMARY KEY,
    order_no TEXT,
    customer_no TEXT,
    customer_name TEXT,
    order_date TEXT,
    invoice_date TEXT,
    warehouse_no TEXT,
    total_cases TEXT,
    total_gross_weight TEXT
);