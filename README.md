## Data Engineering Workflow

```mermaid
flowchart TD
    A[Streaming Data from WebSocket] -->|Deserialize data from Protobuf format\nand insert into Kafka topic| B[(Apache Kafka)]
    C[Apache Airflow]
    CC{Airflow sensor}
    C --> CC --> |Periodically check if new data\nis available in the Kafka topic| B
    B --> CC
    CC --> |Launch pipeline if new data is available| D[Apache Spark pipeline]
    B --> |Stream data from Kafka topic| D
    D --> |Aggregate data into fixed 5 minute windows,\nstart emitting results every 1 minute\nwith 15 minute watermark cutoff for late-arriving data,\nand shut down after 15 minutes of inactivity| E[(PostgreSQL database)]
    

