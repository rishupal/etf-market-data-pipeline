# ETF Market Data Analytics Pipeline

## Overview
This project demonstrates a finance-focused data pipeline that ingests,
validates, transforms, and analyses ETF market price data.

## Business Context
Accurate market data is critical for asset managers to calculate returns,
compare performance against benchmarks, and ensure regulatory compliance.

## Features

## Key Features & Architecture (Vanguard Alignment)

- **Metadata-driven Data Pipeline**: Centralized schema and data catalog management for flexible, scalable data processing.
- **Incremental Data Lakehouse Simulation**: Partitioned, change-aware data loading, designed for easy migration to AWS (S3, Glue, PySpark).
- **Data Quality & Governance**: Automated validation, logging, and documentation to support robust data governance.
- **Monitoring & Reporting**: Integrated logging and health checks for pipeline observability and reliability.
- **Dimensional Modeling**: Example star schema and Data Vault concepts for analytics-ready data structures.
- **Multi-source Data Integration**: Ingests data from flat files, APIs, JSON, and RDBMS formats.
- **Data Visualization**: Sample dashboards and visualizations using Python (matplotlib) and Tableau-ready exports.
- **Collaboration & CI/CD**: Git workflow, contribution guidelines, and simulated CI/CD pipeline for team development.

## Tech Stack
- Python
- Pandas
- Git
- AWS (simulated: S3, Glue, PySpark)
- SQL (for transformation and modeling)
- matplotlib (visualization)
- Tableau (dashboard-ready exports)

## Getting Started
See individual module documentation for usage and extension. The pipeline is modular and ready for cloud migration.

## Tech Stack
- Python
- Pandas
- Git
