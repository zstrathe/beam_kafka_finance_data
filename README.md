## Data Engineering Workflow

```mermaid
flowchart TB
    A(("`**Streaming Data
        from WebSocket**`")) 
    B[("`**Apache Kafka**`")]
    A --> |Deserialize data from Protobuf format\nand insert into Kafka topic| B
    C(("`**Apache Airflow**`"))
    CC["`**Airflow sensor**:
        Periodically check if new data is available in the Kafka topic`"]
    C --> CC --> C
    CC -->  B --> CC
    D["`**Apache Spark pipeline**:
        Aggregate data into fixed 5 minute windows, start emitting results every 1 minute with 15 minute watermark cutoff for late-arriving data, and shut down after 15 minutes of inactivity`"]
    C --> |Launch pipeline if new data is available| D
    B --> |Stream data from Kafka topic| D
    E[("`**PostgreSQL database**`")]
    D --> E
