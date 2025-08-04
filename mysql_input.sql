
-- Customers
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    loyalty_program_id INT,
    gender VARCHAR(10),
    age INT,
    created_at DATE,
    FOREIGN KEY (loyalty_program_id) REFERENCES loyalty_programs(loyalty_program_id)
);

-- Stores
CREATE TABLE stores (
    store_id INT PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100),
    manager_id INT
);

-- Employees
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    role VARCHAR(50),
    store_id INT,
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

-- Products
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    category_id INT,
    brand_id INT,
    supplier_id INT,
    price DECIMAL(10,2),
    created_at DATE,
    season VARCHAR(20),
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Categories
CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Brands
CREATE TABLE brands (
    brand_id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Suppliers
CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    name VARCHAR(100),
    contact_info VARCHAR(100)
);

-- Loyalty Programs
CREATE TABLE loyalty_programs (
    loyalty_program_id INT PRIMARY KEY,
    name VARCHAR(100),
    points_per_dollar INT
);

-- Sales Transactions
CREATE TABLE sales_transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    employee_id INT,
    transaction_date DATE,
    total_amount DECIMAL(10,2),
    payment_id INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (payment_id) REFERENCES payments(payment_id)
);

-- Sales Items
CREATE TABLE sales_items (
    item_id INT PRIMARY KEY,
    transaction_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    tax DECIMAL(10,2),
    FOREIGN KEY (transaction_id) REFERENCES sales_transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Payments
CREATE TABLE payments (
    payment_id INT PRIMARY KEY,
    method VARCHAR(50),
    status VARCHAR(50),
    paid_at DATE
);

-- Inventory
CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY,
    store_id INT,
    product_id INT,
    quantity INT,
    last_updated DATE,
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Stock Movements
CREATE TABLE stock_movements (
    movement_id INT PRIMARY KEY,
    product_id INT,
    store_id INT,
    movement_type VARCHAR(20),
    quantity INT,
    movement_date DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

-- Purchase Orders
CREATE TABLE purchase_orders (
    order_id INT PRIMARY KEY,
    supplier_id INT,
    order_date DATE,
    status VARCHAR(50),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Shipments
CREATE TABLE shipments (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    store_id INT,
    shipped_date DATE,
    received_date DATE,
    FOREIGN KEY (order_id) REFERENCES purchase_orders(order_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

-- Returns
CREATE TABLE returns (
    return_id INT PRIMARY KEY,
    item_id INT,
    reason VARCHAR(100),
    return_date DATE,
    FOREIGN KEY (item_id) REFERENCES sales_items(item_id)
);

-- Promotions
CREATE TABLE promotions (
    promotion_id INT PRIMARY KEY,
    name VARCHAR(100),
    start_date DATE,
    end_date DATE
);

-- Campaigns
CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    name VARCHAR(100),
    budget DECIMAL(10,2),
    start_date DATE,
    end_date DATE
);

-- Customer Feedback
CREATE TABLE customer_feedback (
    feedback_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    product_id INT,
    rating VARCHAR(10),
    comments VARCHAR(255),
    feedback_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Store Visits
CREATE TABLE store_visits (
    visit_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    visit_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

-- Pricing History
CREATE TABLE pricing_history (
    history_id INT PRIMARY KEY,
    product_id INT,
    price DECIMAL(10,2),
    effective_date DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Discount Rules
CREATE TABLE discount_rules (
    rule_id INT PRIMARY KEY,
    product_id INT,
    discount_type VARCHAR(50),
    value DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Tax Rules/////////////////////////////
CREATE TABLE tax_rules (
    tax_id INT PRIMARY KEY,
    product_id INT,
    tax_rate VARCHAR(10),
    region VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);




-- LOAD DATA INTO MYSQL
-- De-activate foreign key checks
SET FOREIGN_KEY_CHECKS = 0;

-- 1. brands
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/brands.csv' INTO TABLE brands FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (brand_id, name);


-- 2. categories
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/categories.csv' INTO TABLE categories FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (category_id, name);

-- 3. loyalty_programs
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/loyalty_programs.csv' INTO TABLE loyalty_programs FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (loyalty_program_id, name, points_per_dollar);

-- 4. suppliers
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/suppliers.csv' INTO TABLE suppliers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (supplier_id, name, contact_info);

-- 5. customers
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/customers.csv' INTO TABLE customers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (customer_id, name, email, phone, loyalty_program_id, gender, age, created_at);

-- 6. products
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/products.csv' INTO TABLE products FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (product_id, name, category_id, brand_id, supplier_id, price, created_at, season);

-- 7. stores
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/stores.csv' INTO TABLE stores FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (store_id, name, location, manager_id);

-- 8. employees
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/employees.csv' INTO TABLE employees FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (employee_id, name, role, store_id);

-- 9. payments
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/payments.csv' INTO TABLE payments FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (payment_id, method, status, paid_at);

-- 10. sales_transactions
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/sales_transactions.csv' INTO TABLE sales_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (transaction_id, customer_id, store_id, employee_id, transaction_date, total_amount, payment_id);

-- 11. sales_items
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/sales_items.csv' INTO TABLE sales_items FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (item_id, transaction_id, product_id, quantity, unit_price, discount, tax);

-- 12. inventory
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/inventory.csv' INTO TABLE inventory FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (inventory_id, store_id, product_id, quantity, last_updated);

-- 13. stock_movements
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/stock_movements.csv' INTO TABLE stock_movements FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (movement_id, product_id, store_id, movement_type, quantity, movement_date);

-- 14. purchase_orders
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/purchase_orders.csv' INTO TABLE purchase_orders FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (order_id, supplier_id, order_date, status);

-- 15. shipments
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/shipments.csv' INTO TABLE shipments FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (shipment_id, order_id, store_id, shipped_date, received_date);

-- 16. returns
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/returns.csv' INTO TABLE returns FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (return_id, item_id, reason, return_date);

-- 17. promotions
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/promotions.csv' INTO TABLE promotions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (promotion_id, name, start_date, end_date);

-- 18. campaigns
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/campaigns.csv' INTO TABLE campaigns FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (campaign_id, name, budget, start_date, end_date);

-- 19. customer_feedback
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/customer_feedback.csv' INTO TABLE customer_feedback FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (feedback_id, customer_id, store_id, product_id, rating, comments, feedback_date);

-- 20. store_visits
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/store_visits.csv' INTO TABLE store_visits FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (visit_id, customer_id, store_id, visit_date);

-- 21. pricing_history
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/pricing_history.csv' INTO TABLE pricing_history FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (history_id, product_id, price, effective_date);

-- 22. discount_rules
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/discount_rules.csv' INTO TABLE discount_rules FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (rule_id, product_id, discount_type, value, valid_from, valid_to);

-- 23. tax_rules
LOAD DATA LOCAL INFILE 'C:/Users/Dell/final_fpt_training_project/retail_data/retail_data/tax_rules.csv' INTO TABLE tax_rules FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS (tax_id, product_id, tax_rate, region);

-- Re-activate foreign key checks
SET FOREIGN_KEY_CHECKS = 1;












