CREATE DATABASE IF NOT EXISTS ecommerce;

-- A single table for all funnel events
CREATE TABLE
    ecommerce.raw_events (
        event_type String,
        user_id UInt32,
        session_id String,
        timestamp DateTime64 (3),
        -- All possible fields from different event types
        page String,
        product_id UInt32,
        action String,
        quantity UInt32,
        order_id String,
        amount Decimal(10, 2),
        status String,
        -- Technical fields
        _topic String,
        _offset UInt64
    ) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMM (timestamp)
ORDER BY
    (timestamp, session_id, user_id, event_type);

-- Enriched data view with labels for VictoriaMetrics
CREATE VIEW
    ecommerce.enriched_events AS
SELECT
    event_type,
    user_id,
    session_id,
    timestamp,
    -- Correcting the source definition
    multiIf (
        event_type IN ('page_view', 'product_click'),
        'view_service',
        event_type IN (
            'add_to_cart',
            'remove_from_cart',
            'update_quantity'
        ),
        'cart_service',
        event_type = 'order_event',
        'order_service',
        'unknown'
    ) as service_source,
    -- Cleared fields
    lower(trim(page)) as page,
    if (
        product_id > 0
        AND product_id < 1000000,
        product_id,
        0
    ) as product_id,
    if (empty (trim(action)), '', lower(trim(action))) as action,
    if (
        quantity > 0
        AND quantity < 100,
        quantity,
        1
    ) as quantity,
    replaceRegexpAll (trim(order_id), '[^a-zA-Z0-9_-]', '') as order_id,
    if (
        amount > 0
        AND amount < 100000,
        amount,
        0
    ) as amount,
    if (empty (trim(status)), '', lower(trim(status))) as status,
    _topic
FROM
    ecommerce.raw_events
WHERE
    user_id > 0;

-- Table for aggregated metrics of product metrics
CREATE TABLE
    ecommerce.product_metrics (
        timestamp DateTime64 (3),
        metric_name String,
        metric_value Float64,
        labels Map (String, String)
    ) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMM (timestamp)
ORDER BY
    (metric_name, timestamp, labels);