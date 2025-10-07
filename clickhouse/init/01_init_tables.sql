CREATE DATABASE IF NOT EXISTS ecommerce;

-- Table for cleared page views
CREATE TABLE
    ecommerce.page_views (
        event_id String DEFAULT generateUUIDv4 (),
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        page String,
        product_id UInt32,
        service_source String DEFAULT 'view_service',
        -- Cleared fields
        is_bot UInt8 DEFAULT 0,
        page_category String,
        _original_topic String
    ) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMM (timestamp)
ORDER BY
    (timestamp, user_id, page);

-- Table for cart events
CREATE TABLE
    ecommerce.cart_events (
        event_id String DEFAULT generateUUIDv4 (),
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        product_id UInt32,
        action Enum8 ('add' = 1, 'remove' = 2, 'update' = 3),
        quantity UInt8,
        service_source String DEFAULT 'cart_service',
        -- Cleared fields
        is_valid UInt8 DEFAULT 1,
        _original_topic String
    ) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMM (timestamp)
ORDER BY
    (timestamp, user_id, action);

-- Table for order events
CREATE TABLE
    ecommerce.order_events (
        event_id String DEFAULT generateUUIDv4 (),
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        order_id String,
        amount Decimal(10, 2),
        status Enum8 ('created' = 1, 'completed' = 2, 'cancelled' = 3),
        service_source String DEFAULT 'order_service',
        -- Cleared fields
        is_fraud_suspected UInt8 DEFAULT 0,
        amount_category String,
        _original_topic String
    ) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMM (timestamp)
ORDER BY
    (timestamp, user_id, status);