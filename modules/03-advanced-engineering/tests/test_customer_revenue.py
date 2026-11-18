from challenges.customer_revenue import clean_events, customer_revenue


def test_clean_events_rejects_bad_rows_and_keeps_latest(spark):
    events = spark.createDataFrame(
        [("e1", "a", "2024-01-01", 100), ("e1", "a", "2024-01-02", 10),
         ("e1", "a", "2024-01-02", 20), ("e2", "b", "2024-01-01", 0),
         (" ", "a", "2024-01-01", 5), ("e3", None, "2024-01-01", 5),
         ("e4", "a", None, 5), ("e5", "a", "2024-01-01", -1),
         ("e6", "a", "2024-01-01", None)],
        "event_id string, customer_id string, event_time string, amount int",
    )
    rows = {(r.event_id, r.amount) for r in clean_events(events).collect()}
    assert rows == {("e1", 20), ("e2", 0)}


def test_revenue_keeps_customers_and_breaks_ties(spark):
    customers = spark.createDataFrame(
        [("a", "N"), ("b", "N"), ("c", "N"), ("d", "S")],
        "customer_id string, region string",
    )
    events = spark.createDataFrame(
        [("e1", "a", "2024-01-01", 5), ("e2", "b", "2024-01-01", 5),
         ("e3", "unknown", "2024-01-01", 100)],
        "event_id string, customer_id string, event_time string, amount int",
    )
    rows = {r.customer_id: (r.revenue, r.position)
            for r in customer_revenue(customers, events).collect()}
    assert rows == {"a": (5, 1), "b": (5, 2), "c": (0, 3), "d": (0, 1)}
