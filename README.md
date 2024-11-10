# FMCG-Sales-Dashboard-2024

FMCG Sales Dashboard tracks all transaction stages of the business: Sell-In (principal to distributor), Sell-Through (distributor to store), and Sell-Out (store to end-user), along with income generated. Data is available with SKU-level granularity, by date and store, providing a detailed view of sales performance.

## Project Overview
This project integrates data from multiple sources to provide a unified and interactive sales performance dashboard using Power BI. The goal is to enable a complete analysis of FMCG sales data, covering the entire transaction journey.

## Key Features
- **Interactive Visualizations**: Power BI dashboard provides insights into sales trends, revenue, and performance at every stage.
- **Data Integration**: Combines 10 SQL Server tables and MongoDB Sell-Out data into a single, unified dataset.
- **ETL Process**: Python scripts are used to extract and load data from SQL Server and MongoDB into Google BigQuery.
- **Data Warehouse**: Google BigQuery serves as the OLAP structure and the main source for analytics and reporting.

## Technical Details
- **Tools Used**: Power BI for visualization, SQL for data integration, Python for ETL, Google BigQuery as the data warehouse, and SQL Server and MongoDB as data sources.
- **Data Processing**: SQL and MongoDB queries unify data from 10 tables and collections, transforming it into a single dataset for analysis.
- **ETL Pipeline**: Python automates the data transfer to Google BigQuery, creating an OLAP-ready structure.

## Example Screenshots
- **Power BI Dashboard**: Include images of interactive dashboards showing sales trends and SKU-level insights.
- **SQL & MongoDB Code Examples**: Show code snippets used to consolidate data from different sources.
