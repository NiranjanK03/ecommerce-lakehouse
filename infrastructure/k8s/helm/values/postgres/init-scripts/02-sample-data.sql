-- Seed a small representative dataset for local development.
-- The full Olist dataset (100k orders) is loaded by scripts/load-olist-data.sh
-- once the cluster is running. This seed covers smoke-test scenarios:
--   - 3 customers, 2 sellers, 3 products
--   - 2 orders with items, payments, and reviews
--   - A mix of order statuses to exercise Silver-layer deduplication

INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state) VALUES
    ('a1b2c3d4-0001-0001-0001-000000000001', 'u1b2c3d4-0001-0001-0001-000000000001', '01310-100', 'sao paulo',    'SP'),
    ('a1b2c3d4-0002-0002-0002-000000000002', 'u1b2c3d4-0002-0002-0002-000000000002', '20040-020', 'rio de janeiro','RJ'),
    ('a1b2c3d4-0003-0003-0003-000000000003', 'u1b2c3d4-0003-0003-0003-000000000003', '30140-110', 'belo horizonte','MG')
ON CONFLICT DO NOTHING;

INSERT INTO sellers (seller_id, seller_zip_code, seller_city, seller_state) VALUES
    ('b1c2d3e4-0001-0001-0001-000000000001', '01001-001', 'sao paulo',    'SP'),
    ('b1c2d3e4-0002-0002-0002-000000000002', '80010-010', 'curitiba',     'PR')
ON CONFLICT DO NOTHING;

INSERT INTO products (product_id, product_category, product_name_length, product_desc_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) VALUES
    ('c1d2e3f4-0001-0001-0001-000000000001', 'electronics',     42, 250, 3, 500,  20, 10, 15),
    ('c1d2e3f4-0002-0002-0002-000000000002', 'bed_bath_table',  30, 180, 2, 1200, 35, 25, 30),
    ('c1d2e3f4-0003-0003-0003-000000000003', 'sports_leisure',  25, 120, 1, 300,  15,  5, 10)
ON CONFLICT DO NOTHING;

INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_customer, order_estimated_delivery) VALUES
    ('d1e2f3a4-0001-0001-0001-000000000001',
     'a1b2c3d4-0001-0001-0001-000000000001',
     'delivered',
     '2018-01-15 14:30:00',
     '2018-01-15 15:00:00',
     '2018-01-22 10:00:00',
     '2018-01-25'),
    ('d1e2f3a4-0002-0002-0002-000000000002',
     'a1b2c3d4-0002-0002-0002-000000000002',
     'shipped',
     '2018-02-01 09:00:00',
     '2018-02-01 09:30:00',
     NULL,
     '2018-02-10')
ON CONFLICT DO NOTHING;

INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) VALUES
    ('d1e2f3a4-0001-0001-0001-000000000001', 1, 'c1d2e3f4-0001-0001-0001-000000000001', 'b1c2d3e4-0001-0001-0001-000000000001', '2018-01-16 00:00:00', 99.90,  12.50),
    ('d1e2f3a4-0001-0001-0001-000000000001', 2, 'c1d2e3f4-0002-0002-0002-000000000002', 'b1c2d3e4-0002-0002-0002-000000000002', '2018-01-16 00:00:00', 149.90, 18.00),
    ('d1e2f3a4-0002-0002-0002-000000000002', 1, 'c1d2e3f4-0003-0003-0003-000000000003', 'b1c2d3e4-0001-0001-0001-000000000001', '2018-02-02 00:00:00', 39.90,   8.50)
ON CONFLICT DO NOTHING;

INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value) VALUES
    ('d1e2f3a4-0001-0001-0001-000000000001', 1, 'credit_card', 3, 262.40),
    ('d1e2f3a4-0002-0002-0002-000000000002', 1, 'boleto',      1,  48.40)
ON CONFLICT DO NOTHING;

INSERT INTO order_reviews (review_id, order_id, review_score, review_comment_title, review_comment_msg, review_creation_date, review_answer_date) VALUES
    ('e1f2a3b4-0001-0001-0001-000000000001', 'd1e2f3a4-0001-0001-0001-000000000001', 5, 'Great!',   'Fast delivery, product as described.', '2018-01-23 00:00:00', '2018-01-24 00:00:00'),
    ('e1f2a3b4-0002-0002-0002-000000000002', 'd1e2f3a4-0002-0002-0002-000000000002', 3, 'Ok',       'Still waiting for delivery.',          '2018-02-05 00:00:00', '2018-02-06 00:00:00')
ON CONFLICT DO NOTHING;

INSERT INTO category_translations (product_category_name, product_category_name_english) VALUES
    ('electronics',    'electronics'),
    ('cama_mesa_banho','bed_bath_table'),
    ('esporte_lazer',  'sports_leisure')
ON CONFLICT DO NOTHING;
