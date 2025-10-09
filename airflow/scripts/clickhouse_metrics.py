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
        WITH minute_aggregation AS (
            SELECT
                toStartOfMinute(timestamp) as minute,
                countDistinctIf(session_id, service_source = 'view_service') as viewing_sessions,
                countDistinctIf(session_id, service_source = 'cart_service') as cart_sessions,
                countDistinctIf(session_id, service_source = 'order_service') as order_sessions,
                sumIf(amount, service_source = 'order_service') as gmv
            FROM ecommerce.enriched_events
            WHERE timestamp >= now() - interval 1 hour
            GROUP BY minute
        )
        -- Conversion Rate
        SELECT
            'ecommerce_conversion_rate' as metric,
            toFloat64(if(viewing_sessions > 0, order_sessions / viewing_sessions, 0)) as value,
            minute as timestamp,
            map('type', 'conversion') as labels
        FROM minute_aggregation
        WHERE viewing_sessions > 0

        UNION ALL

        -- Cart Abandonment Rate
        SELECT
            'ecommerce_cart_abandonment_rate' as metric,
            toFloat64(if(cart_sessions > 0, (cart_sessions - order_sessions) / cart_sessions, 0)) as value,
            minute as timestamp,
            map('type', 'abandonment') as labels
        FROM minute_aggregation
        WHERE cart_sessions > 0

        UNION ALL

        -- GMV
        SELECT
            'ecommerce_gmv' as metric,
            toFloat64(gmv) as value,
            minute as timestamp,
            map('type', 'revenue') as labels
        FROM minute_aggregation
        WHERE gmv > 0
    """

    return client.execute(metrics_query)
