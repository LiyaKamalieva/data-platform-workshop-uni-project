\# Domain: Clickstream



\## Data Product: Top Products (Product Popularity)



\### Owner

Alena (Data Engineer)



\### Purpose

Produce a ranked list of products based on streaming click activity + historical orders.

Used for recommendation and analytics.



\### Sources

\- Kafka topic: aggregated\_clicks (streaming aggregates)

\- DuckDB table: orders (historical orders)



\### Outputs (Published Interfaces)

\- DuckDB table: top\_products (analytical store)

\- Redis hash: top\_products (feature store / low-latency access)



\### Update Frequency / SLA

Every 10 minutes (Airflow DAG: recommendation\_pipeline)



\### Consumers

\- Recommendation service / personalization

\- BI / product analytics



\### Quality \& Monitoring (basic)

\- Pipeline run status tracked in Airflow

\- Data freshness: updated\_at in top\_products

