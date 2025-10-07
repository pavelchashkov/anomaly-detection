-- Kafka tables for each topic, but we combine them into one raw_events
-- Page Views topic
CREATE TABLE
    ecommerce.kafka_page_views (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp Int64,
        page String,
        product_id UInt32
    ) ENGINE = Kafka (
        'kafka:9092',
        'page-views',
        'clickhouse-page-views-consumer',
        'JSONEachRow'
    );

-- Cart Events topic
CREATE TABLE
    ecommerce.kafka_cart_events (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp Int64,
        product_id UInt32,
        action String,
        quantity UInt32
    ) ENGINE = Kafka (
        'kafka:9092',
        'cart-events',
        'clickhouse-cart-consumer',
        'JSONEachRow'
    );

-- Order Events topic
CREATE TABLE
    ecommerce.kafka_order_events (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp Int64,
        order_id String,
        amount Float64,
        status String
    ) ENGINE = Kafka (
        'kafka:9092',
        'order-events',
        'clickhouse-order-consumer',
        'JSONEachRow'
    );

-- Materialized views for joining into a single table
CREATE MATERIALIZED VIEW ecommerce.page_views_consumer TO ecommerce.raw_events AS
SELECT
    event_type,
    user_id,
    session_id,
    toDateTime (timestamp) as timestamp,
    page,
    product_id,
    '' as action,
    0 as quantity,
    '' as order_id,
    0 as amount,
    '' as status,
    'page-views' as _topic,
    _offset
FROM
    ecommerce.kafka_page_views;

CREATE MATERIALIZED VIEW ecommerce.cart_events_consumer TO ecommerce.raw_events AS
SELECT
    event_type,
    user_id,
    session_id,
    toDateTime (timestamp) as timestamp,
    '' as page,
    product_id,
    action,
    quantity,
    '' as order_id,
    0 as amount,
    '' as status,
    'cart-events' as _topic,
    _offset
FROM
    ecommerce.kafka_cart_events;

CREATE MATERIALIZED VIEW ecommerce.order_events_consumer TO ecommerce.raw_events AS
SELECT
    event_type,
    user_id,
    session_id,
    toDateTime (timestamp) as timestamp,
    '' as page,
    0 as product_id,
    '' as action,
    0 as quantity,
    order_id,
    amount,
    status,
    'order-events' as _topic,
    _offset
FROM
    ecommerce.kafka_order_events;