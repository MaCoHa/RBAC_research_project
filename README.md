# Study of Role-Based Access Control Mechanisms

This repository contains benchmarking code developed for a master's thesis focused on evaluating Role-Based Access Control (RBAC) mechanisms in database systems. The experiments were conducted on MariaDB and PostgreSQL databases.

## Repository Structure

- `Performace_test/`: Contains scripts and resources for performance testing.
- `benchmark/`: Includes benchmarking scripts and configurations.
- `databaseSql/`: Holds SQL scripts for database setup and initialization.
- `plots/`: Contains scripts for generating plots and visualizations of benchmarking results.
- `main.py`: The main script to execute the benchmarking experiments.
- `plot.py`: Script for generating plots based on the benchmarking results.
- `.gitignore`: Specifies files and directories to be ignored by git.
- `.python-version`: Specifies the Python version used in the project.
- `pyproject.toml`: Contains project dependencies and build system requirements.
- `uv.lock`: Lock file for package versions.

## Setup and Installation

To set up and run the benchmarking code, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/MaCoHa/Study_of_Role-Based_Access_Control_Mechanisms.git
   cd Study_of_Role-Based_Access_Control_Mechanisms
   ```

2. **Install dependencies:**
   Ensure you have Python installed on your system. It's recommended to use a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
   Then, install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure database connections:**
   Update the configuration files or environment variables to specify the connection details for your databases. The code can be run against local databases or AWS RDS instances. Ensure that the necessary permissions and network access are configured.

## Running the Benchmark

To execute the benchmarking experiments:

1. **Initialize the databases:**
   Insert the relavant data into the .env file
   The database should have a Role0 and a table foo.
3. **Run the main benchmarking script:**
   ```bash
   python main.py
   ```
   This script will perform the benchmarking tests as configured.

4. **Generate plots and visualizations:**
   After running the benchmarks, you can generate plots using:
   ```bash
   python plot.py
   ```
   The resulting visualizations will be saved in the `plots/` directory.

## AWS EC2 and RDS Setup

If you choose to run the benchmarks on AWS:

1. **Launch an EC2 instance:**
   - Select an appropriate instance type based on your performance requirements.
   - Configure security groups to allow access to your RDS instances and, if necessary, SSH access.

2. **Set up RDS instances:**
   - Create MariaDB and PostgreSQL RDS instances.
   - Configure security groups and parameter groups as needed.
   - Note the endpoint addresses and authentication details for use in your benchmarking configuration.

3. **Deploy the benchmarking code to EC2:**
   - SSH into your EC2 instance.
   - Clone this repository and follow the setup instructions above.
   - Ensure that the EC2 instance has the necessary permissions and network access to connect to the RDS instances.
   - Ensure that all the dependencyes are imported, i use the python uv
     ```bash
      sudo apt-get install python3-dev libpq-dev
      sudo apt install python3-pip
      uv pip install psycopg2-binary
      uv pip install psycopg2
      sudo apt install libmariadb-dev
      uv sync
     ```
    - Ensure that the .env file have the RDS database information
    - Ensure that in the Performance_test/run_benchmark.py file the correct database test are uncommented
    - Run the code by
     ```bash
       cd Performance_test
       uv run run_benchmark.py
     ```

## Contributing

Contributions to this project are welcome. If you have suggestions, improvements, or encounter issues, please submit a pull request or open an issue in the repository.

