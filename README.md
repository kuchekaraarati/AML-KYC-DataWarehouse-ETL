# AML & KYC Data Warehouse Analytics Project

## Overview

This project demonstrates the design and implementation of an end-to-end Anti-Money Laundering (AML) and Know Your Customer (KYC) Data Warehouse using Microsoft SQL Server. The solution follows a layered architecture consisting of Raw, Staging, Dimension, and Fact tables to support compliance reporting, customer risk monitoring, and transaction analysis.

The project simulates a banking environment by processing customer, account, and transaction data through ETL workflows and generating analytical insights for AML monitoring.

---

## Objectives

* Build a SQL Server-based AML/KYC Data Warehouse.
* Implement ETL processes for data ingestion and transformation.
* Perform data validation and quality checks.
* Classify customers based on AML risk rules.
* Detect suspicious transactions using predefined monitoring logic.
* Generate analytical insights for compliance reporting.

---

## Technology Stack

* Microsoft SQL Server
* SQL
* Data Warehousing
* ETL Development
* Dimensional Modeling
* Data Quality Validation
* Analytical Reporting

---

## Data Architecture

### Raw Layer

Stores source data loaded into SQL Server.

Tables:

* Raw_Customers
* Raw_Accounts
* Raw_Transactions

### Staging Layer

Intermediate layer used for cleansing and transformation.

Tables:

* Stg_Customers
* Stg_Accounts
* Stg_Transactions

### Dimension Layer

Tables:

* DimCustomer
* DimAccount
* DimDate

### Fact Layer

Tables:

* FactTransactions

### Supporting Tables

Tables:

* AML_HighRiskCountries
* DataQualityIssues

---

## Dataset Summary

| Dataset      | Records |
| ------------ | ------: |
| Customers    |   1,000 |
| Accounts     |   2,000 |
| Transactions |  10,000 |

---

## ETL Workflow

### Step 1: Data Ingestion

Loaded customer, account, and transaction datasets into raw tables.

### Step 2: Staging

Transferred data into staging tables for validation and transformation.

### Step 3: Data Quality Validation

Performed validation checks to identify data inconsistencies and improve data quality.

### Step 4: Dimension Loading

Created and populated:

* DimCustomer
* DimAccount
* DimDate

### Step 5: Fact Loading

Loaded transaction data into FactTransactions using surrogate keys and dimensional relationships.

### Step 6: Analytics & Reporting

Executed analytical SQL queries to generate AML and KYC insights.

---

## Key Business Rules

### Customer Risk Classification

Customers belonging to the following countries are classified as High Risk:

* Iran
* North Korea
* Syria
* Afghanistan

All other customers are classified as Low Risk.

### Suspicious Transaction Detection

Transactions with an amount greater than or equal to 50,000 are flagged as suspicious.

---

## Project Results

### Customer Risk Distribution

| Risk Level | Customers |
| ---------- | --------: |
| High Risk  |        96 |
| Low Risk   |       904 |

### Transaction Statistics

| Metric                  |          Value |
| ----------------------- | -------------: |
| Total Transactions      |         10,000 |
| Suspicious Transactions |          4,997 |
| Total Transaction Value | 500,496,653.49 |

### Transaction Type Analysis

| Transaction Type | Count |   Total Amount |
| ---------------- | ----: | -------------: |
| Deposit          | 3,386 | 168,486,695.43 |
| Withdrawal       | 3,320 | 164,456,751.56 |
| Transfer         | 3,294 | 167,553,206.50 |

### Top Customers by Transaction Value

| Customer          |       Amount |
| ----------------- | -----------: |
| George Cox        | 2,057,831.26 |
| Pamela Pineda     | 2,005,509.76 |
| Stephanie Schmitt | 1,919,618.17 |
| Samantha Spears   | 1,904,361.07 |
| Jeremy Smith      | 1,831,537.25 |

---

## Key Insights

* Processed 1,000 customer records, 2,000 account records, and 10,000 transaction records through a SQL Server Data Warehouse.
* Identified 96 high-risk customers using AML country-based risk classification rules.
* Flagged 4,997 transactions for monitoring using suspicious transaction detection logic.
* Processed over 500 million in transaction value across customer accounts.
* Built a star-schema-based data warehouse using dimension and fact tables.
* Generated analytical insights to support AML compliance monitoring and reporting.

---

## Skills Demonstrated

* SQL Server
* Data Warehousing
* ETL Development
* Data Profiling
* Data Quality Validation
* Data Modelling
* Business Reporting
* AML Analytics
* KYC Monitoring
* Analytical SQL Queries

---

## Future Enhancements

* Looker Studio Dashboard
* Advanced AML Detection Rules
* Customer Risk Scoring Model
* Transaction Pattern Analysis
* Automated ETL Scheduling
* Data Quality Monitoring Framework

---

## Author

Aarati Kuchekar

LinkedIn: [www.linkedin.com/in/aarati-kuchekar-36665b1b2](http://www.linkedin.com/in/aarati-kuchekar-36665b1b2)

GitHub: https://github.com/kuchekaraarati

