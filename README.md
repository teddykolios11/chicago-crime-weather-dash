# Chicago Crime Analytics Platform

This project is an end-to-end big data analytics application built around a **Lambda Architecture**. It ingests, processes, and serves large-scale Chicago crime and weather data using **HDFS, Hive, Spark, HBase**, and a **Node.js web UI** for interactive exploration.

The system supports batch analytics, machine-learning–driven insights, and low-latency queries surfaced through a web interface.

GH link: https://github.com/teddykolios11/chicago-crime-weather-dash

---

## High-Level Architecture

- **Storage Layer**
  - Raw and processed data stored in **HDFS**
    - Located at /tkolios/data/crimes and /tkolios/data/weather on cluster
  - Analytical tables managed in **Hive**
- **Batch / ML Layer**
  - **Spark (Scala)** jobs for feature engineering, modeling, and batch processing
    - Spark ML jar located at /home/hadoop/tkolios/
- **Serving Layer**
  - Model outputs and aggregates written to **HBase**
- **Presentation Layer**
  - **Node.js frontend** for visualization and interaction with results

---

## Repository Structure
```
├── hive/
│   ├── *.hql
│   └── …
├── scala/
│   ├── *.scala
│   └── …
├── tkolios-crime-ui/
│   ├── app.js
│   ├── views/
│   ├── public/
│   ├── package.json
│   └── …
└── README.md
```
---

## Directory Breakdown

### `hive/`
Contains **Hive Query Language (HQL)** scripts used for:

- Creating external and managed Hive tables
- Cleaning and transforming raw crime and weather data
- Joining datasets (e.g., crime + weather)
- Preparing batch-layer tables for downstream Spark processing

These scripts are typically run after data is landed in HDFS.

---

### `scala/`
Contains **Scala / Spark** code responsible for:

- Feature engineering
- Machine learning model training (e.g., temporal or weather-based predictors)
- Batch analytics and aggregations
- Writing model outputs and aggregates from Hive into **HBase** for fast access

These jobs form the **batch layer** of the system.

---

### `tkolios-crime-ui/`
A **Node.js frontend application** that serves as the presentation layer.

Features include:
- Querying precomputed results from HBase
- Displaying analytics, predictions, and feature importance
- Interactive UI components for exploring crime patterns

Key components:
- `app.js`
- `views/`
- `public/`

---

## Typical Workflow

1. **Ingest Data**
   - Load raw datasets (e.g., Chicago crime, weather) into HDFS

2. **Hive Integration**
   - Run HQL scripts in `hive/` to clean, structure, and join data

3. **Batch Processing & ML**
   - Execute Spark jobs in `scala/` to train models and compute aggregates

4. **Serve Results**
   - Write outputs to HBase for low-latency access

5. **Visualize**
   - Launch the Node.js app in `tkolios-crime-ui/` to explore results

---

## Technologies Used

- **HDFS** — distributed storage
- **Hive** — SQL-based data warehousing
- **Spark (Scala)** — batch processing and machine learning
- **HBase** — serving layer / low-latency storage
- **Node.js / Express** — frontend application
- **Mustache / HTML / CSS / JS** — UI rendering

---

## Author

**Teddy Kolios**