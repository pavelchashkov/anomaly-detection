-- 1. Page Views topic with its own clearing logic
CREATE TABLE
    ecommerce.kafka_page_views (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        page String,
        product_id UInt32,
        some_garbage_field String -- example of a dirty field
    ) ENGINE = Kafka (
        'kafka:9092',
        'page-views',
        'clickhouse-page-views-consumer',
        'JSONEachRow'
    );

-- Materialized view with transformation for page views
CREATE MATERIALIZED VIEW ecommerce.page_views_consumer TO ecommerce.page_views AS
SELECT
    user_id,
    session_id,
    timestamp,
    -- Page Cleanup: Remove spaces, convert to lowercase
    lower(trim(page)) as page,
    -- Validation product_id
    if (
        product_id > 0
        AND product_id < 1000000,
        product_id,
        0
    ) as product_id,
    -- Determine the page category
    multiIf (
        page LIKE '%product%',
        'product_page',
        page LIKE '%catalog%',
        'catalog_page',
        page LIKE '%cart%',
        'cart_page',
        page LIKE '%home%',
        'home_page',
        'other'
    ) as page_category,
    -- Detecting bots by session_id
    if (
        session_id LIKE '%bot%'
        OR session_id LIKE '%crawler%',
        1,
        0
    ) as is_bot,
    '_page-views' as _original_topic
FROM
    ecommerce.kafka_page_views
WHERE
    -- Filtering bots
    is_bot = 0;

-- 2. Cart Events topic with its own logic
CREATE TABLE
    ecommerce.kafka_cart_events (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        product_id UInt32,
        action String,
        quantity UInt32,
        garbage_data String -- example of a dirty field
    ) ENGINE = Kafka (
        'kafka:9092',
        'cart-events',
        'clickhouse-cart-consumer',
        'JSONEachRow'
    );

CREATE MATERIALIZED VIEW ecommerce.cart_events_consumer TO ecommerce.cart_events AS
SELECT
    user_id,
    session_id,
    timestamp,
    -- Validation product_id
    if (product_id BETWEEN 1 AND 1000000, product_id, 0) as product_id,
    -- Normalization of action
    if (
        empty (trim(action)),
        'add',
        if (
            action IN ('add', 'remove', 'update'),
            action,
            'add'
        )
    ) as action,
    -- Validation quantity
    least (quantity, 10) as quantity, -- limit the maximum to 10
    -- Validation event
    if (
        user_id > 0
        AND product_id > 0
        AND quantity > 0,
        1,
        0
    ) as is_valid,
    '_cart-events' as _original_topic
FROM
    ecommerce.kafka_cart_events
WHERE
    -- Only valid events
    is_valid = 1;

-- 3. Order Events topic with its own logic
CREATE TABLE
    ecommerce.kafka_order_events (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        order_id String,
        amount Float64,
        status String,
        some_weird_field String -- example of a dirty field
    ) ENGINE = Kafka (
        'kafka:9092',
        'order-events',
        'clickhouse-order-consumer',
        'JSONEachRow'
    );

CREATE MATERIALIZED VIEW ecommerce.order_events_consumer TO ecommerce.order_events AS
SELECT
    user_id,
    session_id,
    timestamp,
    -- Clearing order_id
    replaceRegexpAll (trim(order_id), '[^a-zA-Z0-9_-]', '') as order_id,
    -- Validation amount
    if (
        amount > 0
        AND amount < 100000,
        amount,
        0
    ) as amount,
    -- Normalization status
    if (
        empty (trim(status)),
        'created',
        if (
            status IN ('created', 'completed', 'cancelled'),
            status,
            'created'
        )
    ) as status,
    -- Detect suspicious orders
    if (
        amount > 10000
        OR user_id = 0,
        1,
        0
    ) as is_fraud_suspected,
    -- Categorization by amount
    multiIf (
        amount = 0,
        'zero',
        amount < 1000,
        'small',
        amount < 5000,
        'medium',
        amount < 20000,
        'large',
        'very_large'
    ) as amount_category,
    '_order-events' as _original_topic
FROM
    ecommerce.kafka_order_events
WHERE
    -- Only orders with a valid user_id
    user_id > 0;