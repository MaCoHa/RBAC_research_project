# Study of Role-Based Access Control Mechanisms

This repository contains benchmarking code developed for a master's thesis focused on evaluating Role-Based Access Control (RBAC) mechanisms in database systems. The experiments were conducted on PostgreSQL, MariaDB and Snowflake databases.


## Setup and Installation

To set up and run the benchmarking code, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/MaCoHa/Study_of_Role-Based_Access_Control_Mechanisms.git
   cd Study_of_Role-Based_Access_Control_Mechanisms
   ```

## Install dependencies:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
source $HOME/.local/bin/env
```

```bash
uv venv
```

```bash
source .venv/bin/activate 
```

```bash
sudo apt-get update
```

```bash
sudo apt-get install python3-dev libpq-dev
```

```bash
sudo apt install python3-pip
```

```bash
uv pip install psycopg2-binary
```

```bash
sudo apt-get update && sudo apt-get install -y python3-dev libpq-dev build-essential
```

```bash
sudo apt-get install -y clang
```

```bash
uv pip install psycopg2
```

```bash
sudo apt install libmariadb-dev
```

```bash
uv sync
```

## ⚙️ Script Setup Instructions

Before running any benchmarks, follow these steps to configure and prepare your environment.

---

### 1. Configure environment variables

Copy the provided environment template and fill in your own configuration:

```bash
cp .env.temp .env
```

Edit `.env` to include connection details (e.g., host, port, user, password, database name).

---

### 2. Choose which tests to run

Open the main benchmarking script:

```
Performance_test/run_benchmarks.py
```

Comment out any test functions that you **do not** want to execute. Only the active (uncommented) tests will be run.

---

### 3. If running **locally**

Make sure the database server is running on your machine and properly configured.

Ensure the following exist in your target database **before running** the script:

- A role named: `role0`
- A table named: `foo`

These are required for the benchmarks to execute without error.

---

### 4. If running **on the cloud** (EC2 + RDS)

Use the `<database>_EC2` experiment modules (e.g., `postgres_EC2`, `mariadb_EC2`, etc.).  
These are designed to:

- Run from an **EC2 instance**, and  
- Connect to a corresponding **RDS instance** hosting the target database.

Verify that:

- Your EC2 instance has network access to the RDS instance (check security groups, VPC, etc.)
- The `.env` file includes the correct connection information for your RDS instance
- Any required roles and tables (like `role0` and `foo`) are already created in the RDS-hosted database

---


## ▶️ Running the Benchmark Script

Once you have installed all dependencies and completed the setup steps, simply run the benchmarks with:

```bash
uv run run_benchmarks.py
```


## 🗂️ Project Structure Overview

- **databaseSql**  
  Contains the SQL scripts used to read and process the raw log data from the experiments into a format suitable for plotting.

- **Performance_test**  
  Contains the scripts used to run the benchmarking experiments.

- **plots**  
  Contains the plotted results from the tests performed during the master thesis.


