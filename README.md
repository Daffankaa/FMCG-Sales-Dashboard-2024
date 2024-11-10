# FMCG-Sales-Dashboard-2024

FMCG Sales Dashboard tracks each transaction stage within the business: Sell-In (from principal to distributor), Sell-Through (from distributor to store), and Sell-Out (from store to end-user), alongside income generated. This dashboard provides SKU-level data granularity, segmented by date and store, for an in-depth view of sales performance.

## Project Overview
This project integrates data from multiple sources to provide a unified and interactive sales performance dashboard using Power BI. The goal is to enable a complete analysis of FMCG sales data, covering the entire transaction journey.

## Key Features
- **Interactive Visualizations**: Power BI dashboards enable dynamic filtering, drill-downs, and trend analysis, providing actionable insights on sales trends, revenue, and performance across all transaction stages.
- **Data Integration**: Combines data from 10 SQL Server tables with MongoDB Sell-Out data to form a comprehensive dataset, covering all business transaction stages in a single view.
- **ETL Process**: Python scripts automate data extraction and loading from SQL Server and MongoDB into Google BigQuery, ensuring data consistency and reducing manual work.
- **Data Warehouse**: Google BigQuery serves as the primary OLAP structure, acting as the centralized source for analytics and reporting across teams.

## Technical Details
- **Tools Used**: Power BI for visualization, SQL for data integration, Python for ETL, Google BigQuery as the data warehouse, and SQL Server and MongoDB as data sources.
- **Data Sources and Integration**: Data is sourced from SQL Server (transactional data) and MongoDB (Sell-Out data). SQL and MongoDB queries unify data across 10 tables and collections, transforming it into a single analytical dataset.
- **ETL Pipeline**: Python automates the data extraction, transformation, and load processes, transferring data to Google BigQuery and preparing it for OLAP analysis.
- **OLAP Structure**: Google BigQuery is structured as a data warehouse for fast querying and large-scale data processing, enabling complex aggregations and multi-dimensional analysis.

## Example Screenshots
- **Power BI Dashboard**: This section showcases the main Power BI dashboard, displaying interactive visualizations of sales data segmented by SKU, date, and store.
  
  ![Power BI Dashboard](https://github.com/user-attachments/assets/f429f042-2907-4e06-8caa-7e22ce030034)
  ![Power BI Dashboard](https://github.com/user-attachments/assets/0e903645-df6f-42dc-8cfe-3db843cfb568)
  ![Power BI Dashboard](https://github.com/user-attachments/assets/dee5575b-e4c8-4440-8024-ba12f89533df)

- **Python Database Relation Code Examples**: These examples illustrate Python code used to handle the relationships and ETL processes from SQL Server and MongoDB to Google BigQuery.

  ![Python Code Example](https://github.com/user-attachments/assets/384536ac-70a2-4fdc-acd2-a1ca96050741)
  ![Python Code Example](https://github.com/user-attachments/assets/94541a85-2c76-4eaa-b1ff-571cf9909430)
  ![Python Code Example](https://github.com/user-attachments/assets/aac0e836-655a-4257-8964-c490731fb480)

- **SQL Query Final Raw Data for Python Database Relation**: This section contains SQL examples for unifying data in Google BigQuery for efficient OLAP processing.

  ![SQL Example](https://github.com/user-attachments/assets/85eacecd-a4ee-4ae8-8eaf-2e93119dd73b)
  ![SQL Example](https://github.com/user-attachments/assets/8dcff7fc-7b0f-46c9-ae8b-d246730d3b45)

- **SQL Query Final Raw Data for Power BI Dataset**: SQL examples used to prepare the final dataset for Power BI dashboard analysis.

  ![SQL Example](https://github.com/user-attachments/assets/24563f2a-d5a8-4b3a-ae1a-e14d94f5afda)

## Setup Instructions
1. **Clone the Repository**: Download the repository to your local machine.
2. **Configure Python ETL**: Update the Python ETL script configuration with your Google BigQuery and MongoDB credentials.
3. **Run ETL Pipeline**: Execute the Python script to load data from SQL Server and MongoDB into Google BigQuery.
4. **Connect Power BI**: Use Power BI’s native Google BigQuery connector to connect to the consolidated dataset and start building dashboards.

---

This README provides a structured overview of the project, with clear steps for replication and visuals to showcase your work.
