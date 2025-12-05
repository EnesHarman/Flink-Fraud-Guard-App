# Fraud Guard

Streaming fraud detection demo built with Apache Flink, Kafka, and PostgreSQL. A Flink job consumes transactions from Kafka, detects multiple fraud patterns in real time, and stores alerts in PostgreSQL. A small Python generator can be used to produce sample transactions.

## What’s inside
- `docker-compose.yml` – local stack (Zookeeper, Kafka, Kafdrop UI, Flink JobManager/TaskManager, PostgreSQL).
- `fraud-engine/` – Flink job sources and Maven project.
  - `src/main/java/com/fraudguard/FraudDetectorJob.java` – entry point.
  - `functions/*` – fraud detection logic (frequency, location, CEP pattern).
  - `sink/PostgresSink.java` – JDBC sink to Postgres.
  - `data-ingestor.py` – optional Python transaction generator (Kafka producer).

## Fraud signals implemented
- High frequency: >5 transactions per user within a 10s sliding window (slide 5s).
- Impossible travel: successive transactions imply speed >800 km/h.
- Spend sequence (CEP): two small (<50) transactions followed by a large (>1000) one within 60s.

## Prerequisites
- Docker & Docker Compose
- Java 17 and Maven (to build the job jar)
- Python 3 (for the optional generator) with `kafka-python` and `Faker`

## Quick start
1) Build the job jar  
```bash
cd fraud-engine
mvn clean package -DskipTests
```
Creates `fraud-engine/target/fraud-engine-0.1.jar`.

2) Start the stack  
```bash
docker-compose up -d
```
Services: Kafka (`9092` mapped), Kafdrop (`9000`), Flink UI (`8081`), Postgres (`5432`).

3) Create the Kafka topic (once)  
```bash
docker exec kafka kafka-topics --create --topic transactions \
  --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1
```

4) Submit the Flink job  
```bash
# copy the shaded jar into the JobManager
docker cp fraud-engine/target/fraud-engine-0.1.jar jobmanager:/opt/flink/usrlib/

# run the job
docker exec jobmanager flink run -c com.fraudguard.FraudDetectorJob \
  /opt/flink/usrlib/fraud-engine-0.1.jar
```
Monitor at http://localhost:8081.

5) (Optional) Generate sample traffic  
```bash
pip install kafka-python Faker
python fraud-engine/data-ingestor.py
```
The generator sends JSON transactions to `transactions` on `localhost:9092`.

6) Inspect results  
- Kafka topics/UI: http://localhost:9000  
- Postgres alerts:  
```bash
docker exec -it postgres psql -U flinkuser -d fraudguard_db \
  -c "SELECT * FROM alerts ORDER BY alert_id DESC LIMIT 20;"
```

## Data contract
Transactions (Kafka, JSON):
```json
{
  "transactionId": "uuid",
  "userId": "User-1",
  "amount": 42.5,
  "timestamp": 1717685000000,   // epoch millis, event time
  "lat": 37.78,
  "lon": -122.4
}
```

Alerts (Postgres `alerts` table):
- `alert_id` (text, primary key)
- `user_id` (text)
- `alert_type` (text)
- `message` (text)

Create the table if it does not exist:
```sql
CREATE TABLE IF NOT EXISTS alerts (
  alert_id TEXT PRIMARY KEY,
  user_id TEXT,
  alert_type TEXT,
  message TEXT
);
```

## Development notes
- Target Java 17; build with `mvn clean package`.
- Kafka source uses `kafka:29092` (internal Docker DNS). Producers outside Docker should use `localhost:9092`.
- Watermarks allow 5s out-of-orderness; idle sources marked after 10s.
- Adjust fraud thresholds in `functions/` (e.g., `HighFrequencyDetector`, `LocationDetectionFunction`, `FraudSequenceFunction`).

## Teardown
```bash
docker-compose down -v
```
This stops containers and removes the Postgres volume.

