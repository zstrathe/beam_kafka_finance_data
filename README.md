## Data Engineering Workflow

```mermaid
flowchart TD
    A[Streaming Data from WebSocket] -->|Protobuf deserialization| B[Apache Kafka]
    C[Apache Airflow]
    B --> C
    C --> |Airflow sensor checks Kafka topic for new data| D[Apache Spark pipeline]
    B --> |Data read from Kafka| D
    D --> |Data aggregated into fixed 5 minute windows| E[PostgreSQL database]
    
