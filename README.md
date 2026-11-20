# Offer Backend Service

## Overview

Offer Backend Service is a Java-based Spring Boot application responsible for processing and maintaining sports betting offers, including events, markets, outcomes, and associated odds.

The application consumes updates from Apache Kafka, maintains the current offer state in memory, and uses PostgreSQL for persistent storage.

It also implements Kafka checkpoint and recovery mechanisms to support restoring previously persisted application state after a restart.

## Tech Stack

- Java 21
- Spring Boot 4
- Spring Web
- Spring Kafka
- Apache Kafka 4.2.2
- PostgreSQL 17
- Spring JDBC (`JdbcTemplate`)
- Jackson 3
- Flyway
- Maven
- Docker / Docker Compose

## Prerequisites

Before running the application, ensure the following tools are installed:

- Java Development Kit (JDK) 21
- Docker Engine and Docker Compose
- Git
- Maven is optional because the project includes the Maven Wrapper (`./mvnw`)

Verify your installation:

```bash
java -version
docker --version
docker compose version
git --version
```

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/pmaksimovic98/offer-backend-service.git
cd offer-backend-service
```

If the repository is private, make sure your GitHub account has the required access.

### 2. Start Apache Kafka

The application requires a running Kafka broker accessible at `localhost:9092`.

If you already have the project's Kafka container, start it:

```bash
docker start offer-kafka
```

For a fresh local installation, create a Kafka container:

```bash
docker run -d \
  --name offer-kafka \
  -p 127.0.0.1:9092:9092 \
  apache/kafka:4.2.2
```

Verify that Kafka is running:

```bash
docker ps --filter name=offer-kafka
```

Wait until the Kafka broker is ready before creating topics.

### 3. Create Kafka topics

The application uses two Kafka topics:

| Topic | Partitions | Purpose |
|---|---:|---|
| `market` | 1 | Market and MarketOutcome updates |
| `event` | 2 | Event, EventMarket, and EventMarketOutcome updates |

Create the topics:

```bash
docker exec offer-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic market \
  --partitions 1 \
  --replication-factor 1
```

```bash
docker exec offer-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic event \
  --partitions 2 \
  --replication-factor 1
```

Verify the topics:

```bash
docker exec offer-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic market
```

```bash
docker exec offer-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic event
```

Existing topics are not recreated. Verify that their partition counts match the required configuration.

### 4. Configure PostgreSQL

For persistent storage and Kafka checkpoint recovery, use the `postgres` Spring profile.

Create a `.env.postgres` file in the project root if it does not already exist:

```dotenv
PG_USERNAME=offer_app
PG_PASSWORD=replace-with-a-strong-local-password
PG_JDBC_URL=jdbc:postgresql://127.0.0.1:55433/offer_state
```

Replace the example password with your own password.

The PostgreSQL Docker configuration uses:

| Setting | Value |
|---|---|
| Container | `offer-postgres` |
| Database | `offer_state` |
| Username | `offer_app` |
| Container port | `5432` |
| Host port | `55433` |

Ensure `docker-compose.postgress.yml` maps PostgreSQL to the corresponding host port:

```yaml
ports:
  - "127.0.0.1:55433:5432"
```

The Compose service must pass the configured credentials as PostgreSQL environment variables, including `POSTGRES_USER` and `POSTGRES_PASSWORD`.

Do not commit `.env.postgres` to version control. Add it to `.gitignore`.

### 5. Start PostgreSQL

From the project root:

```bash
docker compose \
  --env-file .env.postgres \
  -f docker-compose.postgress.yml \
  up -d
```

Verify the container:

```bash
docker ps --filter name=offer-postgres
```

Verify the published port:

```bash
docker port offer-postgres 5432/tcp
```

Expected:

```text
127.0.0.1:55433
```

**Note:** PostgreSQL initializes its database user and password when the data volume is first created. Changing `PG_PASSWORD` later does not automatically change an existing database user's password.

### 6. Load application environment variables

On Linux, load the PostgreSQL configuration:

```bash
set -a
source .env.postgres
set +a
```

Configure the Kafka connection and application HTTP port:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP_ID=offer-backend-service
export SERVER_PORT=8088
```

Port `8088` is the recommended local HTTP port in this README. You can use another available port.

### 7. Build and test

Run the complete Maven verification lifecycle:

```bash
./mvnw clean verify
```

Expected result:

```text
BUILD SUCCESS
```

This compiles the application and executes the tests configured in the Maven build.

### 8. Run the application

**Recommended — PostgreSQL persistence and recovery enabled:**

```bash
./mvnw spring-boot:run \
  -Dspring-boot.run.profiles=postgres
```

The application connects to the configured PostgreSQL database and Kafka broker.

On startup, Flyway validates and applies pending database migrations. The application initializes its state, restores persisted snapshots when valid checkpoints exist, prepares Kafka recovery positions, and starts the consumers.

**Run without the PostgreSQL profile:**

```bash
./mvnw spring-boot:run
```

This uses the application's default profile and configuration rather than explicitly enabling PostgreSQL persistence.

The startup behavior depends on the configuration in `application.yml`.

### 9. Verify the application

Check the Spring Boot console for a successful startup message and the configured HTTP port.

You can also verify PostgreSQL connectivity:

```bash
docker exec offer-postgres \
  psql -U offer_app -d offer_state \
  -c "SELECT current_database(), current_user;"
```

Inspect database migrations:

```bash
docker exec offer-postgres \
  psql -U offer_app -d offer_state \
  -c "SELECT version, description, success
      FROM flyway_schema_history
      ORDER BY installed_rank;"
```

Inspect the persisted state:

```bash
docker exec offer-postgres \
  psql -U offer_app -d offer_state \
  -c "
    SELECT COUNT(*) AS markets
    FROM offer_market_state;

    SELECT COUNT(*) AS events
    FROM offer_event_state;
  "
```

Inspect saved Kafka checkpoint offsets:

```bash
docker exec offer-postgres \
  psql -U offer_app -d offer_state \
  -c "
    SELECT topic, partition_id, next_offset
    FROM offer_checkpoint_offset
    ORDER BY topic, partition_id;
  "
```

**Important:** A Kafka checkpoint stores the next offset to process, not the last processed offset.

## Environment Variables

| Variable | Example | Description |
|---|---|---|
| `SERVER_PORT` | `8088` | Spring Boot HTTP port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP_ID` | `offer-backend-service` | Kafka consumer group |
| `PG_USERNAME` | `offer_app` | PostgreSQL username |
| `PG_PASSWORD` | Set locally | PostgreSQL password |
| `PG_JDBC_URL` | `jdbc:postgresql://127.0.0.1:55433/offer_state` | PostgreSQL JDBC URL |
| `OFFER_INITIAL_STATE_PATH` | See `application.yml` | Optional initial-state file override, if supported by the active configuration |

Environment variables are read by Spring Boot where corresponding configuration placeholders are defined.

## Kafka Processing

The application consumes two types of updates.

**Market updates** describe a betting market and its possible outcomes. Market messages are mapped into domain objects and applied to the current offer state.

**Event updates** describe sporting events and their associated markets and odds. Event updates support partial modifications through the existing application update model.

Kafka messages use a JSON payload and should include a record key matching the corresponding entity identifier where required by the consumer contract.

For example, an Event with `id = e2e-event-001` must use the Kafka record key `e2e-event-001`.

Market and Event messages are processed independently. When PostgreSQL persistence is enabled, the application maintains persisted state snapshots together with Kafka checkpoint metadata.

## Troubleshooting

### HTTP port already in use

Choose another application port:

```bash
export SERVER_PORT=8089
```

Restart the Spring Boot application.

### PostgreSQL password authentication failed

Verify that the password in `.env.postgres` matches the password configured for the existing PostgreSQL user.

Recreating a Docker container without deleting its persistent volume does not reset existing PostgreSQL credentials.

### PostgreSQL port already in use

Check which process occupies the configured port:

```bash
sudo ss -ltnp '( sport = :55433 )'
```

If necessary, change the Docker Compose host-port mapping and update `PG_JDBC_URL` to match.

### Kafka connection failed

Check Kafka status:

```bash
docker ps --filter name=offer-kafka
```

Check broker logs:

```bash
docker logs --tail 100 offer-kafka
```

Verify the configured value of `KAFKA_BOOTSTRAP_SERVERS`.

### Kafka message repeatedly fails

Check the Spring Boot logs for deserialization, validation, or application errors.

A failed message may be retried from the same Kafka offset. Do not reset consumer offsets or delete topics to hide processing failures.

### Recovery problems

Ensure that the PostgreSQL state and checkpoint offsets belong to the same Kafka history.

Do not clear PostgreSQL checkpoint tables independently of Kafka topics unless performing an intentional, controlled reset of a disposable test environment.

## Stopping the Application

Stop Spring Boot with `Ctrl+C`, or use Stop in your IDE.

Stop the PostgreSQL Compose service:

```bash
docker compose \
  --env-file .env.postgres \
  -f docker-compose.postgress.yml \
  down
```

This does not delete named Docker volumes unless explicitly requested with additional destructive options.

Stop Kafka:

```bash
docker stop offer-kafka
```
OpenAPI / Swagger UI
API documentation is available at:
Swagger UI: http://localhost:8088/swagger-ui.html

## References

- [Spring Boot Documentation](https://docs.spring.io/spring-boot/reference/)
- [Spring Kafka Documentation](https://docs.spring.io/spring-kafka/reference/)
- [Apache Kafka Docker Documentation](https://kafka.apache.org/42/getting-started/docker/)
- [Flyway Documentation](https://documentation.red-gate.com/fd)
