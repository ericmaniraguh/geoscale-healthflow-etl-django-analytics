
---

# GeoScalable Malaria ETL & Analytics Platform

## 1. Project Overview

The **GeoScalable Malaria ETL & Analytics Platform** is a full-stack, data-driven system built using **Django (Python)** to automate the **collection, transformation, integration, and analysis** of malaria-related **health, environmental, and geospatial data**.

The platform enables scalable and reproducible malaria surveillance by integrating heterogeneous data sources into automated ETL pipelines and interactive analytics dashboards, supporting **evidence-based public health decision-making** at fine geographic scales (down to village level).

### Key Objectives

* Automate ingestion of multi-source malaria datasets
* Integrate health, environmental, and geospatial data
* Enable village-level malaria risk analysis
* Support scalable deployment across countries and regions

---

## 2. Integrated Data Sources

### Health & Surveillance Data

* **Health Center Laboratory Data** (CSV / Excel uploads)
* **HMIS Data** (national malaria statistics via API or file uploads)

### Geospatial Data

* Country administrative boundaries (Shapefiles uploaded as ZIP)
* Slope data (GeoTIFF / Shapefiles from OpenTopography)

### Environmental Data

* Temperature
* Precipitation

---

## 3. Scalability & Adaptability

The platform is **country-agnostic** and reusable beyond Rwanda.

To adapt it to another country or region, only the following need to be updated:

* Administrative boundary datasets
* Slope / elevation data
* Health Center and HMIS datasets

No core application or ETL code changes are required.

---

## 4. Technology Stack

### Backend & APIs

* **Django (Python)** – core application logic, REST APIs, authentication, ETL triggers

### Frontend

* HTML
* Bootstrap
* JavaScript (AJAX)

### Databases

* **MongoDB**

  * Raw, unfiltered data storage
  * Stores uploads, shapefiles, weather data, and metadata
* **PostgreSQL**

  * Cleaned and transformed staging data
  * Optimized for analytics and dashboards

### ETL & Infrastructure

* **Apache Airflow** – ETL orchestration and scheduling
* **Docker & Docker Compose** – containerized deployment

### Authentication

* OTP-based authentication
* Google OAuth2

### Visualization

* Interactive dashboards (charts & maps)
* External **ArcGIS public dashboard** integration

---

## 5. Data Flow & ETL Workflow

### High-Level ETL Workflow

```
Raw Data (CSV / Excel / API / Shapefiles)
            ↓
        MongoDB
   (Raw Data + Metadata)
            ↓
     Apache Airflow ETL
            ↓
   PostgreSQL (Staging DB)
            ↓
     Analytics Dashboards
```

---

### 📌 ETL & Analytics Workflow Diagram

> ⚠️ **Important**:
> To ensure the diagram is visible on GitHub, it must be stored outside Django static files.

**Recommended location (already supported by GitHub):**

```
docs/images/geoscale_workflow_diagram.png
```

**README reference:**

```markdown
![GeoScalable Malaria ETL Workflow](docs/images/geoscale_workflow_diagram.png)
```

![GeoScalable Malaria ETL Workflow](docs/images/geoscale_workflow_diagram.png)

**Figure 1.** High-level data ingestion, ETL orchestration, and analytics workflow for the GeoScalable Malaria Platform.


---

## 6. Core Features

### User Capabilities

* User registration with **OTP-based authentication**
* Upload Health Center and HMIS datasets
* Select datasets for ETL execution
* View interactive analytics dashboards:

  * Malaria cases and positivity rates
  * Weather–malaria correlations
  * Village- and sector-level summaries
* Filter analytics by **year, location, and dataset**

### Admin (Superuser) Capabilities

* Upload country boundaries and slope geospatial data
* Manage users and permissions
* Trigger and monitor ETL pipelines
* Access full analytics and system configurations

---

## 7. Database Design & ETL Logic

### MongoDB (Raw Storage)

* Stores unfiltered raw datasets
* Automatically creates databases and collections if absent
* Stores upload metadata (file name, upload date, dataset type)
* Enables flexible dataset selection for ETL processing

### PostgreSQL (Staging & Analytics)

Stores cleaned and transformed datasets, including:

* Health Center laboratory records
* HMIS aggregated statistics
* Merged geospatial–health datasets
* Weather station data

Optimized for analytical queries and dashboards.

### ETL Automation

* Apache Airflow detects newly ingested data
* Executes selected ETL pipelines
* Dashboards update automatically upon ETL completion

---

## 8. Prerequisites

* Docker Desktop
* Python 3.8+
* Git
* MongoDB Atlas account (or local MongoDB)
* PgAdmin 4 (optional, recommended)

---

## 9. Installation & Setup

### 9.1 Clone Repository

```bash
git clone https://github.com/cylab-africa/geoscale-etl-django-analytics.git
cd geoscale-etl-django-analytics
```

### 9.2 Python Environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### 9.3 Environment Configuration

```bash
cp .env.example .env
```

Configure:

* MongoDB credentials
* PostgreSQL credentials
* Email service
* Google OAuth2 keys

---

## 10. Docker Services

```bash
docker-compose up -d
docker ps
```

---

## 11. Initialize Django Application

```bash
python manage.py migrate
python manage.py createsuperuser
python manage.py load_rwanda_locations
python manage.py runserver
```

### Remote / Server Access

```bash
python3 manage.py runserver 0.0.0.0:8000
```

---

## 12. Analytics Dashboards

* Interactive filtering by Province, District, Sector, Year
* Displays malaria cases, positivity rates, weather correlations
* Dashboards update automatically after ETL completion

---

## 13. Research Outputs & Public Dashboards

### 📊 ArcGIS Public Dashboard

🗺️ **ArcGIS Malaria Analytics Dashboard**
👉 [https://www.arcgis.com/apps/dashboards/7ea4f9fb5db14a2c8da2d76c079869fe](https://www.arcgis.com/apps/dashboards/7ea4f9fb5db14a2c8da2d76c079869fe)

**Highlights**

* Village- and sector-level malaria patterns
* Spatial clustering of positivity rates
* Integration of environmental and geographic factors
* Designed for policymakers, researchers, and public health teams

### 📄 Research Paper (In Preparation)

**Title**
*Micro-Geographic Mapping of Malaria Risk Factors in Bugesera District, Rwanda: A Village-Level Analysis*

**Status**
Under peer review (manuscript submitted, not yet published)


**Supervision**
Prof. Carine  Pierrette Mukamakuza & Upanzi DPI Network Leaders 
---

## 14. Testing

```bash
python manage.py test
```

---

## 15. Dataset Checklist & Access

### Datasets Used

* RBC Rwanda – HMIS Malaria Data
* Meteo Rwanda – Temperature & Precipitation
* Slope Data – OpenTopography
* Kamabuye Health Centre Laboratory Records
* Community Interviews Data

📎 **Dataset Access**
👉 Access restricted to authorized internal users (CMU-Africa). [https://drive.google.com/drive/folders/1YSp-dIGq5IXtxdaA3TAd2ArqipwjhovH](https://drive.google.com/drive/folders/1YSp-dIGq5IXtxdaA3TAd2ArqipwjhovH)

---

## 16. Acknowledgements

* Kamabuye Health Centre
* Community Health Workers (CHWs) – Kamabuye
* Rwanda Biomedical Centre (RBC)
* Upanzi Network
* Carnegie Mellon University Africa

---

## 17. Best Practices & Notes

* Never commit `.env` files
* Run migrations after pulling updates
* Verify geospatial uploads before ETL
* Use Airflow for scheduled data refresh

---

## 18. Project Ownership & Attribution

**Developed and Maintained by**

**Eric Maniraguha**
Research Associate – **Upanzi Network (CyLab Africa)**


This project contributes to applied research in **geospatial health analytics, malaria surveillance, and scalable ETL systems** for public health decision support.

---
