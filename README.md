Got it 👍 — here’s your **clean, icon-free, concise `README.md`**, formatted professionally for GitHub or project documentation.

---

# GeoScalable Malaria ETL & Analytics Platform

**GeoScalable-Malaria-Project** is a full-stack, open-source platform that automates malaria data collection, transformation, and analytics.
It integrates **Django, Airflow, PostgreSQL, MongoDB, Elasticsearch, and Kibana** to generate actionable health insights from multiple data sources such as Health Centers, HMIS APIs, shapefiles, slope, and weather data.

---

## System Overview

```text
Users (Health Center & HMIS uploads)
        │
        ▼
Django Backend (ETL APIs + OTP-secured login)
        │
        ▼
PostgreSQL (cleaned data)
        │
        ▼
Airflow Automation (scheduled ETL jobs)
        │
        ▼
Elasticsearch → Kibana Dashboards (real-time analytics)
```

---

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/cylab-africa/geoscale-etl-django-analytics.git
cd geoscale-etl-django-analytics
```

### 2. Backend Setup

```bash
cd django_backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Update `.env` with your MongoDB, PostgreSQL, and Elasticsearch credentials.

### 3. Run Django Server

```bash
python manage.py migrate
python manage.py createsuperuser
python manage.py runserver 0.0.0.0:8000
```

---

## Core Features

### User Management with OTP

* Secure signup and login using email-based OTP verification
* Role-based access:

  * **Admin:** manages shapefiles, slope data, and user accounts
  * **User:** uploads Health Center or HMIS datasets (CSV/Excel)

### Rwanda Location Seeding

Load all provinces, districts, and sectors:

```bash
python manage.py load_rwanda_locations
```

### Automated ETL Pipelines

Endpoints for data ingestion, cleaning, and transformation:

| Process            | Endpoint                           | Description                         |
| ------------------ | ---------------------------------- | ----------------------------------- |
| Health Center Data | `/etl/hc/lab-data/`                | Process malaria lab data            |
| Weather Data       | `/etl/weather/prec-temp/data/`     | Combine temperature & precipitation |
| HMIS API Data      | `/etl/api/malaria/calculate/`      | Compute malaria positivity rates    |
| Shapefiles         | `/etl/shapefile/admin-boundaries/` | Extract administrative boundaries   |
| Slope Data         | `/etl/extract/slope-geojson/`      | Generate slope-based GeoJSON data   |

---

## Example Request

```bash
curl -X POST http://localhost:8000/etl/hc/lab-data/ \
  -H "Content-Type: application/json" \
  -d '{
    "years": [2021, 2022, 2023],
    "district": "Bugesera",
    "sector": "Kamabuye",
    "save_to_postgres": true
  }'
```

**Response Example**

```json
{
  "success": true,
  "message": "Successfully processed 21,126 records",
  "analytics": {
    "total_records": 21126,
    "positive_cases": 1250,
    "positivity_rate": 5.91
  },
  "postgres": { "saved": true, "table_name": "hc_lab_data_bugesera_kamabuye_2021_2023" }
}
```

---

## Technologies

| Layer      | Tools                                 |
| ---------- | ------------------------------------- |
| Backend    | Django, Django REST Framework, Python |
| Automation | Apache Airflow                        |
| Databases  | PostgreSQL, MongoDB Atlas             |
| Analytics  | Elasticsearch, Kibana                 |
| Frontend   | HTML, Bootstrap, JavaScript (AJAX)    |
| Auth       | Email OTP Verification                |
| Deployment | Docker Compose (Airflow + ELK)        |

---

## Dashboards

| Role        | URL                       | Description                              |
| ----------- | ------------------------- | ---------------------------------------- |
| Admin       | `/admin-dashboard/`       | Manage shapefiles, slope data, and users |
| User        | `/user-dashboard/`        | Upload HC & HMIS CSV/Excel files         |
| ETL Monitor | `/etl-dashboard/`         | Run and monitor ETL jobs                 |
| Kibana      | `http://<server-ip>:5601` | View analytics dashboards                |

---

## Example Workflow

1. User uploads malaria data via the web interface.
2. Django ETL module cleans and stores data in PostgreSQL.
3. Airflow DAGs schedule regular ETL and Elasticsearch indexing.
4. Kibana visualizes malaria hotspots and temporal trends.

---

## Optional Commands

Load Rwanda administrative boundaries:

```bash
python manage.py load_rwanda_locations
```

Test MongoDB connection:

```bash
python manage.py shell
from accounts.utils import test_mongo_connection
```

---

## License

MIT License © 2025
Developed under **CyLab Africa / Upanzi DPI Network**
Lead Maintainer: **Eric Maniraguha**

---
