# Airflow Data Pipeline Readme

This is an Apache Airflow data pipeline for fetching data from an API, processing it, and loading it into a PostgreSQL database. The pipeline consists of several tasks orchestrated using Apache Airflow's DAGs.

## Overview

The purpose of this pipeline is to:
1. Check the availability of an API endpoint.
2. Fetch data from the API.
3. Convert the JSON response to a CSV file.
4. Check for the existence of the CSV file.
5. Move the CSV file to the /tmp directory.
6. Load the data from the CSV file into a PostgreSQL database.
7. Execute a Spark job on the loaded data.
8. Retrieve and log the results from the PostgreSQL database.

## Components

### Dependencies

- Python (3.x)
- Apache Airflow
- pandas
- PostgreSQL
- Spark (with the corresponding PostgreSQL driver)

### DAG Structure

- `api_check`: HTTP sensor task to check the availability of the API endpoint.
- `task_get_giveaways`: Fetch data from the API and convert it to a JSON response.
- `task_to_csv`: Convert the JSON data to a CSV file.
- `check_file`: Check for the existence of the CSV file.
- `move_file2tmp`: Move the CSV file to the /tmp directory.
- `to_postgres`: Load the data from the CSV file into a PostgreSQL database.
- `spark_submit`: Execute a Spark job on the loaded data.
- `from_postgres`: Retrieve data from the PostgreSQL database.
- `log_result_final`: Log the results obtained from the PostgreSQL database.

### How to Use

1. Ensure that you have Python, Apache Airflow, PostgreSQL, and Spark set up in your environment.

2. Create an Airflow connection named 'airflow_http' for the API endpoint.

3. Set up your PostgreSQL connection in Airflow with the connection ID 'postgres'.

4. Place this DAG script in your Airflow DAGs folder.

5. Trigger the DAG to execute, or set up a schedule interval as needed.

## Configuration

- The DAG is configured to run once a day (`schedule_interval=timedelta(days=1)`).
- Modify the endpoint and file paths as needed in the DAG definition.
- Customize the PostgreSQL table and Spark job as per your requirements.

## Author

- Your Name

Feel free to reach out with any questions or issues related to this pipeline.

Happy data processing!
