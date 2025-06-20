# RetailRocket Graph-Bundle Recommender

Build a complete **bundle-recommendation engine** from raw RetailRocket logs to a production REST API that serves “Frequently Bought Together” bundles in <150 ms.

---

## Data Architecture (Medallion)

| Layer | Purpose | Main objects |
|-------|---------|--------------|
| **Bronze** | Raw CSV import | `events`, `item_properties_part*` |
| **Silver** | Cleansed & modeled | `purchases`, `co_edges`, `item_features` |
| **Gold** | Business-ready | `bundle_mv` (indexed view = top-3 bundle items per product) |


---

## Deliverables

* **Data Engineering** – SQL Server ingest & cleanse, dbt-sqlserver models, nightly jobs  
* **Graph ML** – GraphSAGE trained with PyTorch Geometric  
* **API & DevOps** – FastAPI, Docker, CI/CD (GitHub Actions), Azure deploy  
* **Analytics** – Power BI dashboard (lift, CTR, AOV)

---

## Requirements

### Data Warehouse
* Source : RetailRocket (~300 MB)  
* Quality : dedup, enforce PK/FK, covering indexes  
* Model : anchor-product bundles in `gold.bundle_mv`  
* Docs : autogen via `dbt docs`

### Machine Learning
* Task : link-prediction with negative sampling  
* Model : 3-layer GraphSAGE, 128-dim embeddings, early-stop on Recall@20  
* Artifacts : `model.pt`, `embeddings.npy`

### API & Deployment
* Endpoint : `GET /recommend?item_id=<int>` → top-3 bundles + lift  
* Latency : p95 < 150 ms (Azure B1 container)  
* Cost : < 30 USD/month (Azure SQL DB S0 + Container Apps)

---

## Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├──
│   ├──
│   ├──
│   ├──
│   ├──
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```
---

## Quick Start
```bash
# clone & env vars
git clone https://github.com/<you>/bundle-reco.git
cd bundle-reco
cp .env.example .env

# ingest data + build Bronze/Silver/Gold
sqlcmd -S localhost -i sql/01_import.sql
dbt run --profiles-dir dbt/

# train GraphSAGE (~15 min CPU)
python model/train.py

# launch API locally
docker-compose up --build

# test
curl "http://localhost:8000/recommend?item_id=287864"
```
## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.


## About Me

I’m a computer science student at Université de Montréal, passionate about data engineering, machine learning, and cloud architecture. I build real-world data pipelines, dashboards, and smart systems that solve actual business problems.

Currently focused on:
- Designing end-to-end data platforms (SQL, Airflow, dbt, AWS)
- Building ML-powered security tools and real-time analytics systems
- Improving my skills in MLOps and scalable AI deployment
