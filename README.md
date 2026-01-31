# Airflow Pipeline Project

## 1. Kafka Monitoring DAG
### Health Check Implementation
- [x] Create Kafka HealthCheck Operator
  - [x] Check broker connectivity
  - [x] Verify topic availability
  - [x] Monitor consumer group status

### Data Flow Monitoring
- [x] Implement data flow checks
  - [x] Monitor message throughput
  - [x] Check consumer lag
  - [x] Verify message processing rates
  - [x] Set up alerting thresholds

## 2. Spark Monitoring DAG
### Health Check Implementation
- [x] Create Spark HealthCheck Operator (YARN-based)
  - [x] Check master, worker available
  - [x] Monitor resource (core cpu, ram) of workers

### Check data output
- [x] Check data had been inserted to db
  - [x] Query fact_view for recent records (90-minute window)

## 3. Alert System
### Alert Configuration
- [x] Set up alerting framework
  - [x] Create Telegram notification utility
  - [x] Set up Telegram notifications (configure bot + sending message to a group)
  - [x] Integrate on_failure_callback into DAGs

---

## Summary
**Completed:**
- ✅ Kafka Health Check (broker connectivity)
- ✅ Spark Health Check (YARN cluster monitoring)
- ✅ Database ingestion verification
- ✅ Telegram alert system

**Pending (Optional enhancements):**
- ⏳ Kafka advanced monitoring (topic availability, consumer lag, throughput)
- ⏳ Alerting thresholds configuration

**Setup Required:**
- 📝 Configure Telegram bot credentials (see TELEGRAM_SETUP.md)
