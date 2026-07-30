INSERT INTO stores (
    store_code,
    store_name,
    city,
    state
)
VALUES
    ('RJ-MES-01', 'Loja Mesquita', 'Mesquita', 'RJ'),
    ('RJ-NIL-01', 'Loja Nilópolis', 'Nilópolis', 'RJ'),
    ('RJ-RIO-01', 'Loja Centro', 'Rio de Janeiro', 'RJ');

INSERT INTO products (
    sku,
    product_name,
    category,
    unit_price
)
VALUES
    ('SKU-001', 'Perfume Aurora 100ml', 'Perfumaria', 299.90),
    ('SKU-002', 'Perfume Horizon 100ml', 'Perfumaria', 349.90),
    ('SKU-003', 'Body Splash Flora 200ml', 'Cuidados Pessoais', 89.90),
    ('SKU-004', 'Creme Corporal Essence', 'Cuidados Pessoais', 69.90),
    ('SKU-005', 'Kit Presente Aurora', 'Kits', 399.90),
    ('SKU-006', 'Kit Presente Horizon', 'Kits', 449.90);

INSERT INTO promotions (
    promotion_name,
    discount_percentage,
    start_date,
    end_date
)
VALUES
    (
        'Campanha de lançamento',
        10,
        CURRENT_DATE,
        CURRENT_DATE + 30
    ),
    (
        'Promoção de kits',
        15,
        CURRENT_DATE + 5,
        CURRENT_DATE + 20
    );

INSERT INTO inventory (
    store_id,
    product_id,
    stock_quantity,
    safety_stock
)
SELECT
    stores.store_id,
    products.product_id,
    50 + (stores.store_id * 10) + (products.product_id * 5),
    20
FROM stores
CROSS JOIN products;

INSERT INTO demand_forecasts (
    store_id,
    product_id,
    forecast_date,
    forecast_quantity,
    model_version
)
SELECT
    stores.store_id,
    products.product_id,
    CURRENT_DATE + days.day_number,
    10 + stores.store_id + products.product_id + days.day_number,
    'baseline-v1'
FROM stores
CROSS JOIN products
CROSS JOIN generate_series(0, 13) AS days(day_number);
