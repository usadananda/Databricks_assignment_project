# 🎓 School Enrollment & Education Performance Analytics Platform

An End-to-End Data Analytics Application built using:

- Python
- Apache Spark (Databricks)
- Delta Lake
- Unity Catalog
- Apache Airflow (Astro CLI)
- Databricks SQL Dashboard

---

## 📌 Project Overview

This project implements a complete Medallion Architecture (Bronze → Silver → Gold) to analyze school enrollment and performance data.

The platform enables:

- Year-over-Year Enrollment Analysis
- Gender-Based Distribution Insights
- Regional Performance Comparison
- School Ranking Metrics
- Automated Workflow Orchestration

---

## 🏗️ Architecture
CSV Data
↓
Bronze Layer (Raw Ingestion)
↓
Silver Layer (Cleaned & Validated Data)
↓
Gold Layer (Aggregated Business KPIs)
↓
Databricks SQL Dashboard
↑
Apache Airflow (Workflow Orchestration)


---

## 🥉 Bronze Layer

- Raw CSV ingestion using Spark
- Stored as Delta Table
- Partitioned by `year`
- Managed using Unity Catalog

---

## 🥈 Silver Layer

- Removed duplicates
- Null value validation
- Data range checks:
  - Grade between 1–12
  - Enrollment > 0
  - Performance score between 0–100
- Standardized region values
- Added audit timestamp
- Fail-fast validation logic

---

## 🥇 Gold Layer

Created Business KPIs:

1. **Yearly Enrollment Trend**
2. **Gender Distribution**
3. **Region Performance Analysis**
4. **School Ranking**
5. Enrollment Growth Rate (optional enhancement)

All Gold tables stored as Delta format.

---

## 🔄 Workflow Automation

### Databricks Job
- Bronze → Silver → Gold execution sequence

### Apache Airflow (Astro CLI)
- DAG triggers Databricks job
- Daily scheduling
- Retry configuration
- Dependency management

Airflow Operator Used:
`DatabricksRunNowOperator`

---

## 📊 Dashboard

Built using Databricks SQL Dashboard.

Visualizations include:
- Line Chart: Yearly Enrollment Trend
- Bar Chart: Gender Distribution
- Bar Chart: Regional Performance
- Table: Top 20 School Ranking
- Interactive Year & Region Filters

---

## 🧪 Data Validation

Implemented validation checks in:

- Silver Layer (data quality)
- Gold Layer (business metrics)

Fail-fast logic ensures pipeline stops if invalid data is detected.

---

## ⚙️ Technologies Used

- Python
- PySpark
- Azure Databricks
- Delta Lake
- Unity Catalog
- Apache Airflow (Astro CLI)
- Databricks SQL

---

## 🚀 How to Run

### Databricks
1. Upload CSV file
2. Run Bronze notebook
3. Run Silver notebook
4. Run Gold notebook
5. Open SQL Dashboard

### Airflow (Astro CLI)
```bash
astro dev start

---

📈 Key Insights Generated

Enrollment growth trend over multiple years

Gender participation comparison

Top-performing regions

Highest ranked schools

Enrollment growth percentage

📌 Conclusion

This project demonstrates:

End-to-end data engineering pipeline

Medallion architecture implementation

Data validation best practices

Workflow automation using Airflow

Business-focused analytics dashboard