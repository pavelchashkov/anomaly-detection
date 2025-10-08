from clickhouse_driver import Client


def get_ecommerce_metrics():
    client = Client(
        host="clickhouse",
        port=9000,
        user="admin",
        password="password",
        database="ecommerce",
    )

    metrics_query = """
    WITH minute_data AS (
        SELECT
            toStartOfMinute(now()) as timestamp,
            countDistinctIf(session_id, service_source = 'view_service') as viewing_sessions,
            countDistinctIf(session_id, service_source = 'cart_service') as cart_sessions,
            countDistinctIf(session_id, service_source = 'order_service') as order_sessions,
            sumIf(amount, service_source = 'order_service') as gmv
        FROM ecommerce.enriched_events
        WHERE timestamp >= now() - interval 2 minute
    )
    SELECT
        'ecommerce_conversion_rate' as metric,
        if(viewing_sessions > 0, order_sessions / viewing_sessions, 0) as value,
        timestamp,
        map('type', 'conversion') as labels
    FROM minute_data

    UNION ALL

    SELECT
        'ecommerce_cart_abandonment_rate' as metric,
        if(cart_sessions > 0, (cart_sessions - order_sessions) / cart_sessions, 0) as value,
        timestamp,
        map('type', 'abandonment') as labels
    FROM minute_data

    UNION ALL

    SELECT
        'ecommerce_gmv' as metric,
        gmv as value,
        timestamp,
        map('type', 'revenue') as labels
    FROM minute_data
    """

    return client.execute(metrics_query)
