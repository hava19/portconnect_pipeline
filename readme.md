# PortConnect — Smart Container Port Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white"/>
  <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white"/>
  <img src="https://img.shields.io/badge/EMQX-00B4D8?style=for-the-badge&logo=mqtt&logoColor=white"/>
</p>

> A production-grade, end-to-end data engineering platform simulating real-time container port operations — from IoT sensor ingestion to analytical gold-layer reporting.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Pipeline](#data-pipeline)
- [Medallion Architecture](#medallion-architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Modules](#modules)
- [Data Schema](#data-schema)

---

## Overview

**PortConnect** is a comprehensive data engineering simulation of a modern container terminal. It models the full lifecycle of container movements — from crane discharge/load events and gate in/out tracking to real-time IoT streaming and batch analytics.

The platform demonstrates:

- **Real-time IoT data ingestion** from crane sensors via MQTT
- **Event-driven streaming** through Confluent Kafka
- **Medallion architecture** (Bronze → Silver → Gold) on Databricks Delta Live Tables
- **Batch pipeline orchestration** with Apache Airflow
- **Data lake storage** on AWS S3
- **REST API simulation** for gate and terminal endpoints via FastAPI

---

## 🎥 Demo

[![Watch Demo](https://img.shields.io/badge/Watch%20Demo-Google%20Drive-blue?style=for-the-badge&logo=googledrive)](https://drive.google.com/file/d/1AiXYNmQwXOCU_FcKYbn9LxYzimwki6Hy/view?usp=sharing)


## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                │
│                                                                      │
│  🏗️ Crane & Gate IoT Simulator     🌐 Manifest & Vessel API Sim     │
│     (Python MQTT Publisher)           (FastAPI Endpoints)            │
└──────────────┬───────────────────────────────┬──────────────────────┘
               │ MQTT /crane/events             │ REST API
               ▼                               ▼
┌──────────────────────┐           ┌───────────────────────┐
│   EMQX MQTT Broker   │           │    Apache Airflow      │
│   (IoT Gateway)      │           │    (Orchestrator)      │
│   Rule Engine        │           │    DAG Scheduling      │
└──────────┬───────────┘           └───────────┬───────────┘
           │ Kafka Producer                     │ DAGs
           ▼                                   ▼
┌──────────────────────────────────────────────────────────┐
│                  Confluent Kafka Cluster                  │
│                                                          │
│   crane_events │ gate_in_events │ gate_out_events        │
└──────────────────────────┬───────────────────────────────┘
                           │ Spark Structured Streaming
                           ▼
┌──────────────────────────────────────────────────────────┐
│                   AWS S3 (Data Lake)                     │
│   s3://portconnect/                                      │
│   ├── raw/           Bronze layer landing zone           │
│   ├── processed/     Silver layer                        │
│   └── analytics/     Gold layer                          │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│        Databricks Delta Live Tables (DLT)                │
│                                                          │
│  ┌─────────┐    ┌─────────┐    ┌──────────────────────┐ │
│  │ Bronze  │ →  │ Silver  │ →  │        Gold          │ │
│  │ Raw JSON│    │ Parsed  │    │ Container Flow       │ │
│  │ Kafka   │    │ Typed   │    │ Crane Performance    │ │
│  │ Payload │    │ Schema  │    │ Terminal KPIs        │ │
│  └─────────┘    └─────────┘    └──────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **IoT Broker** | EMQX MQTT (Docker) | Receive sensor data from crane simulators |
| **Message Queue** | Confluent Kafka Cloud | Event streaming and service decoupling |
| **Data Lake** | AWS S3 | Raw and processed data storage |
| **Data Warehouse** | Databricks (Delta Lake) | Analytical data store |
| **Processing** | Apache Spark — Databricks DLT | Batch and streaming transformation |
| **Orchestration** | Apache Airflow | DAG scheduling and pipeline management |
| **API Simulation** | FastAPI | Simulate gate in/out terminal endpoints |
| **IoT Simulation** | Python + Paho MQTT | Generate realistic crane and gate events |
| **Infrastructure** | Docker | Local EMQX broker deployment |

---

## Data Pipeline

### Streaming Pipeline (Real-time)

```
Crane Simulator (mqtt_crane_stream.py)
    │  MQTT publish to /crane/events every 5s
    ▼
EMQX Broker — Rule Engine
    │  Forward to Kafka Producer
    ▼
Confluent Kafka → crane_events topic
    │  Spark Structured Streaming
    ▼
Databricks DLT
    ├── crane_events_bronze    (raw JSON)
    └── crane_events_silver    (parsed, typed, flattened)
```

### Batch Pipeline (Orchestrated)

```
Airflow DAG (dag_fetch_api.py)
    │
    ├── Gate In Simulator  → FastAPI → gate_in_events  (Kafka)
    └── Gate Out Simulator → FastAPI → gate_out_events (Kafka)
                                           │
                                           ▼
                                   Databricks DLT
                                   ├── gate_in_events_silver
                                   └── gate_out_events_silver
```

---

## Medallion Architecture

### 🟫 Bronze — Raw Ingestion

| Table | Source | Description |
|---|---|---|
| `crane_events_bronze` | Kafka `crane_events` | Raw crane sensor JSON from MQTT |
| `gate_in_events_bronze` | Kafka `gate_in_events` | Raw truck gate entry events |
| `gate_out_events_bronze` | Kafka `gate_out_events` | Raw truck gate exit events |

### 🥈 Silver — Cleaned & Typed

| Table | Description |
|---|---|
| `crane_events_silver` | Parsed crane events with typed schema, OCR and container_ref flattened |
| `gate_in_events_silver` | Cleaned gate in events with typed timestamps |
| `gate_out_events_silver` | Cleaned gate out events with typed timestamps |

### 🥇 Gold — Business Analytics

| Table | Type | Description |
|---|---|---|
| `gold_container_export_flow_streaming` | Streaming | Export flow: gate in → crane LOAD. Yard dwell time + container status |
| `gold_container_import_flow_streaming` | Streaming | Import flow: crane DISCHARGE → gate out. Yard dwell time + container status |
| `gold_crane_performance` | Streaming | Hourly crane productivity: moves/hour, cycle time, anomaly rate per crane |
| `gold_anomaly_summary` | Streaming | Anomaly tracking per crane per day for maintenance and safety monitoring |
| `gold_terminal_daily_summary` | Materialized View | Daily terminal KPI: export, import, crane throughput and dwell time combined |
| `gold_gate_hourly_traffic` | Streaming | Hourly gate traffic volume for queue management and peak hour analysis |

### Gold Layer — Key Metrics

| Metric | Description |
|---|---|
| `yard_dwell_time_hours` | Time from gate in to crane load (export) or crane discharge to gate out (import) |
| `dwell_time_category` | `NORMAL` <24h / `DELAYED` <48h / `LONG_DELAY` <72h / `CRITICAL` >72h |
| `container_status` | `LOADED_TO_VESSEL` / `DISCHARGE_FROM_VESSEL` / `IN_YARD` / `UNKNOWN` |
| `crane_anomaly` | `WEIGHT_EXTREME` / `SPEED_EXTREME` / `MISSING_OCR` |
| `performance_rating` | `GOOD` / `SLOW` / `FAIR` / `POOR` based on cycle time and anomaly rate |
| `anomaly_severity` | `LOW` / `MEDIUM` / `HIGH` based on anomaly frequency per day |
| `peak_category` | `NORMAL` / `BUSY` / `PEAK` based on hourly gate vehicle count |
| `total_throughput` | Total containers processed per terminal per day |

---

## Project Structure

```
portconnect/
│
├── stream/
│   ├── emqx/                        # EMQX Docker config
│   ├── mqtt_crane_stream.py          # Crane IoT simulator (MQTT publisher)
│   └── gate_in_out_stream.py         # Gate in/out event simulator
│
├── batch/
│   ├── data/
│   │   ├── manifest_import.json      # Import container manifest
│   │   ├── manifest_export.json      # Export container manifest
│   │   └── yard_log.json             # Generated yard movement log
│   ├── gate_in_stream.py             # Gate in batch simulator
│   └── gate_out_stream.py            # Gate out batch simulator
│
├── api/
│   └── main.py                       # FastAPI gate endpoint simulation
│
├── spark/
│   ├── crane_stream.py               # DLT: crane bronze → silver
│   ├── gate_in_stream.py             # DLT: gate in bronze → silver
│   ├── gate_out_stream.py            # DLT: gate out bronze → silver
│   └── terminal_operations.py        # DLT: all gold layer tables
│
├── airflow/
│   └── dags/
│       └── dag_fetch_api.py          # Main Airflow DAG
│
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker
- Python 3.10+
- Confluent Cloud account
- AWS account with S3 bucket
- Databricks workspace

### 1. Start EMQX Broker

```bash
docker run -d \
  --name emqx \
  -p 1883:1883 \
  -p 18083:18083 \
  emqx/emqx:latest
```

Access dashboard at `http://localhost:18083` — default credentials: `admin / public`

### 2. Configure EMQX → Confluent Bridge

In EMQX Dashboard:

1. **Data Integration → Connectors → Create → Kafka Producer**
   - Bootstrap Server: `<your-confluent-bootstrap-server>:9092`
   - Authentication: SASL Plain, Enable TLS, TLS Verify OFF
2. **Data Integration → Rules → Create**
   ```sql
   SELECT * FROM "/crane/events"
   ```
3. Add Action → Kafka Producer → Topic: `crane_events`

### 3. Install Python Dependencies

```bash
pip install paho-mqtt confluent-kafka fastapi uvicorn apache-airflow
```

### 4. Run Crane Simulator

```bash
cd stream/
python mqtt_crane_stream.py
```

### 5. Start FastAPI Gate Simulator

```bash
cd api/
uvicorn main:app --reload --port 8000
```

### 6. Start Airflow

```bash
airflow scheduler --daemon
airflow webserver --port 8080
```

### 7. Deploy Databricks DLT Pipeline

1. Upload all files from `spark/` to Databricks
2. Create a Delta Live Tables pipeline
3. Set storage location to your S3 bucket
4. Run the pipeline

---

## Modules

### 🏗️ Crane IoT Simulator

Simulates **8 cranes** (4 DISCHARGE, 4 LOAD) publishing realistic sensor events every 5 seconds:

- Mechanical telemetry: spreader height, trolley position, hoist speed
- Load cell weight varying by container type (Dry, Reefer, ISO Tank, Flat Rack, etc.)
- OCR scanning: 86% SUCCESS / 9% LOW_CONFIDENCE / 5% FAILED
- Anomaly injection at 5% rate: `WEIGHT_EXTREME`, `SPEED_EXTREME`, `MISSING_OCR`
- GPS coordinates randomized around real crane positions

### 🚪 Gate Simulator

Simulates truck gate movements tied to the crane yard log:

- Gate in events linked to future crane LOAD operations
- Gate out events for discharged containers leaving the terminal
- Realistic timestamps with 1–4 hour yard dwell simulation

### ⚡ Spark DLT Pipeline

Streaming and batch transformations with schema enforcement and data quality expectations. Stream-stream joins use watermarks to handle late-arriving data:

```python
gate_in = spark.readStream.table("gate_in_events_silver") \
    .withWatermark("gate_in_timestamp", "2 hours")

crane = spark.readStream.table("crane_events_silver") \
    .withWatermark("crane_timestamp", "2 hours")
```

### 🌐 FastAPI Endpoints

REST API simulating terminal gate system:

```
POST /gate/in     Register truck gate entry
POST /gate/out    Register truck gate exit
GET  /yard/log    Query container yard status
```

---

## Data Schema

### Crane Event (MQTT Payload)

```json
{
  "event_id": "CRN-20260423-000001",
  "timestamp": "2026-04-23T08:00:00+00:00",
  "crane_id": "CRANE-01",
  "operation": "DISCHARGE",
  "spreader_height_m": 24.5,
  "trolley_position_m": 12.3,
  "hoist_speed_mps": 0.85,
  "cycle_time_sec": 95,
  "load_cell_kg": 22400,
  "spreader_locked": true,
  "gps_lat": -7.194589,
  "gps_lng": 112.891456,
  "ocr": {
    "container_no": "MSCU1234567",
    "confidence_score": 0.98,
    "status": "SUCCESS"
  },
  "container_ref": {
    "container_id": "uuid",
    "container_number": "MSCU1234567",
    "container_type": "1",
    "container_type_name": "Dry",
    "container_size": "40",
    "container_category": "import",
    "seal_number": "SEAL123"
  },
  "anomali_type": null
}
```

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<p align="center">Built with ❤️ for learning modern data engineering — PortConnect © 2026</p>