# ETL Pipeline for Amman Digital Market

## Overview
This project builds a Python ETL pipeline that extracts data from PostgreSQL, transforms it with Pandas, validates data quality, and loads the final customer analytics summary into a new PostgreSQL table and a CSV file.

## Setup
1. Start PostgreSQL with Docker:
   ```bash
   docker run -d --name postgres-m3-int -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=amman_market -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data postgres:15-alpine