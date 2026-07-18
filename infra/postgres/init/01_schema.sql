CREATE TABLE stores (
    store_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_code VARCHAR(20) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state CHAR(2) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku VARCHAR(30) NOT NULL UNIQUE,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE promotions (
    promotion_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    promotion_name VARCHAR(150) NOT NULL,
    discount_percentage NUMERIC(5, 2) NOT NULL
        CHECK (
            discount_percentage >= 0
            AND discount_percentage <= 100
        ),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_promotion_period
        CHECK (end_date >= start_date)
);

CREATE TABLE orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id INTEGER NOT NULL REFERENCES stores(store_id),
    order_status VARCHAR(20) NOT NULL
        CHECK (
            order_status IN (
                'CREATED',
                'PAID',
                'CANCELLED',
                'SHIPPED',
                'DELIVERED'
            )
        ),
    order_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_amount NUMERIC(14, 2) NOT NULL DEFAULT 0
        CHECK (total_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL
        REFERENCES orders(order_id)
        ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount_amount NUMERIC(12, 2) NOT NULL DEFAULT 0
        CHECK (discount_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_product_per_order
        UNIQUE (order_id, product_id)
);

CREATE TABLE inventory (
    store_id INTEGER NOT NULL REFERENCES stores(store_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    stock_quantity INTEGER NOT NULL DEFAULT 0
        CHECK (stock_quantity >= 0),
    safety_stock INTEGER NOT NULL DEFAULT 0
        CHECK (safety_stock >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id, product_id)
);

CREATE TABLE inventory_movements (
    movement_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id INTEGER NOT NULL REFERENCES stores(store_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    movement_type VARCHAR(20) NOT NULL
        CHECK (
            movement_type IN (
                'IN',
                'OUT',
                'ADJUSTMENT',
                'RETURN'
            )
        ),
    quantity INTEGER NOT NULL CHECK (quantity <> 0),
    reason VARCHAR(255),
    movement_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE demand_forecasts (
    forecast_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id INTEGER NOT NULL REFERENCES stores(store_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    forecast_date DATE NOT NULL,
    forecast_quantity NUMERIC(12, 2) NOT NULL
        CHECK (forecast_quantity >= 0),
    model_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_forecast_version
        UNIQUE (
            store_id,
            product_id,
            forecast_date,
            model_version
        )
);

CREATE OR REPLACE FUNCTION update_modified_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER stores_updated_at
BEFORE UPDATE ON stores
FOR EACH ROW
EXECUTE FUNCTION update_modified_timestamp();

CREATE TRIGGER products_updated_at
BEFORE UPDATE ON products
FOR EACH ROW
EXECUTE FUNCTION update_modified_timestamp();

CREATE TRIGGER orders_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION update_modified_timestamp();

CREATE TRIGGER inventory_updated_at
BEFORE UPDATE ON inventory
FOR EACH ROW
EXECUTE FUNCTION update_modified_timestamp();

CREATE INDEX idx_orders_store_date
    ON orders(store_id, order_date);

CREATE INDEX idx_order_items_product
    ON order_items(product_id);

CREATE INDEX idx_inventory_movements_date
    ON inventory_movements(movement_date);

CREATE INDEX idx_forecasts_date
    ON demand_forecasts(forecast_date);
