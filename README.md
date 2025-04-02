# CASE_BEES

Instructions:
1. API: -> I used and read the API documentation.
2. Orchestration Tool: -> I prefer to use Airflow to orchestrate this data pipeline.
3. Language: ->  I used Python for data transformations.
4. Containerization:-> I used Docker Compose to deploy Airflow on my machine with a .yml file.
5. Data Lake Architecture: -> I stored my data in a folder called data_lake, and the file names follow the medallion architecture structure.
6. Monitoring/Alerting:->
- I would configure email alerts (Airflow has a native setting for this) in case of failure.
- I would set up retries to re-execute the DAG in case of failure, increasing the wait time between attempts.
- I would create a step to validate whether the extracted data from the API follows the schema of the previous file.
- I would set up detailed logs inside the Python functions, including timestamps and execution identifiers, and centralized them in CloudWatch, for example, to monitor all DAGs.
- I would configure a health check in the docker-compose.yml file to restart the container in case of failure.
7. Repository: -> Architecture Decisions
My architectural choices were based on the case study requirements:
- I created an Airflow DAG using the PythonOperator, which triggers a series of Python functions. These functions use the requests library to fetch data recursively from the API, retrieving the maximum number of records per page according to the API documentation.
- The raw data is stored in JSON format in the first layer of the data lake.
- Next, I segment the data into smaller tables based on the state, storing them in the silver_lake folder. This facilitates queries, reduces data size per table, and helps eliminate possible duplicates. Finally, I perform an aggregation based on location and type in the gold layer.
- I initially tested the API calls in a .ipynb notebook. Once they worked correctly, I migrated the logic to a .py file and built a standard Airflow DAG.
- To schedule the DAG:
  - Deployed a Docker container using a docker-compose.yml file from the official Airflow site.
  - Modified the configuration to use Celery in local mode because I deployed the container via WSL, where access permissions were an issue.
  - Specified the data lake directory within the Airflow folder.
  - To start Airflow, I ensured that Docker and Docker Compose were installed, then executed: docker-compose up -d
  - Accessed localhost:8080, logged in with airflow as both username and password, and activated the DAG. The DAG runs automatically and updates the data_lake folder accordingly.
9. Cloud Services: -> I did not use cloud service, everything can be do on your own machine
