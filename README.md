Hi there! Welcome to my learning logs.

In this guide, I will share how I developed an ELT pipeline based on a designed dimensional model for a supply chain business, using a case study. 

For the full story about the case study and how I designed the data warehouse, you can check out my article on Medium here:

---
---

# Objective

**In this repository, I’ll focus specifically on how I developed the ELT pipeline, including:**

- Developing the ELT script
  
- Managing data transformations with DBT

- Orchestrate and automate the pipeline with Airflow

- Create the dashboard based on the result for analytics/reporting 

---
---

# Case Study Background

let's assume that I already do a requirements gathering with the stakeholder and user and we were reach and agreement about the data warehouse design. In summary here's the current company condition, problem and their needs for the context:


---

# Pipeline Workflow

Before diving into the main discussion, take a look at the image below. This illustrates the workflow I followed to build this project.

- ## How the pipeline works
  
  - ### Load Task
    The Load task load the data source from aonther database in Snowflake and loads it into the staging schemas in the warehouse database:
  
    - **Staging Schema**: Stores the raw data exactly as it came from the source database.
  
  - ### Transform Task

    The Transform Task performs data transformations based on the design of the data warehouse. The transformations are done using DBT (Data Build Tool) and run it through the DAG using Cosmos package, which helps organize and automate the data processing steps.

    In this step, the raw data from the staging schema is processed and transformed to match the structure and requirements of the data warehouse. Once transformed, the data is loaded into the final schema, which is used by business users for analysis and reporting.

- ## Why use this workflow?

  This workflow was designed based on the business requirements provided by stakeholders in the case study. If you're curious about their specific needs, you can refer to [my article]() for more details.

  Here’s why this workflow fits the scenario:

  - **Aligned with business needs**

    Stakeholders requested a low-cost, scalable, and easy-to-understand solution. This pipeline delivers on those points while allowing room for experimentation before making larger investments.

  - **Efficiency for small data volumes**

    With only ~90,000 rows and slow growth of around 5% per month, the pipeline handles data extraction and loading quickly without needing additional layers or complex systems.

  - **Simplicity and maintainability**

    The design ensures that users can easily understand, use, and maintain the pipeline with minimal technical barriers. This is critical for a team starting small or exploring new possibilities.

- ## Trade-off

  While this workflow is simple and efficient, it does have some limitations:

  - **No raw data backup in warehouse database**

    If a transformation fails or a bug occurs, the data in the staging schema could be affected. This means you might need to re-extract the data from the source. This isn’t a major problem for small datasets, but as the data grows, it might become more time-consuming and inconvenient.

  - **Limited scalability for large datasets**

    If the bookstore experiences rapid growth, this pipeline might need adjustments to handle the increased data volume. At that point, having a dedicated public layer as a raw data archive would become crucial to ensure scalability and reliability.

---
---

# Dataset Overview

I used a sample dataset from Snowflake called TPCH_SF10. This dataset related to a supply chain business. Check here to see the full documentation about the dataset: [Snowflake sample data documentation](https://docs.snowflake.com/en/user-guide/sample-data-tpch) 

---
---

Before starting, take a look at the requirements and preparations below:

# Requirements

- OS:
    - Linux
    - WSL (Windows Subsystem For Linux)
      
- Tools:
    - Docker
    - Snowflake
    - DBT
    - Airflow (Astronomer)
    - Looker Studio
      
- Programming Language:
    - Python
    - SQL
      
- Python Library:
    - dbt-snowflake
    - astronomer-cosmos
    - apache-airflow-providers-snowflake
    - python-dotenv

---

# Preparations

- ## Setup project environment

  Create and activate python environment to isolate project dependencies.
  
  ```
  python -m venv your_project_name         
  source your_project_name/bin/activate    # On Windows: your_project_name\Scripts\activate
  ```

- ## Setup Snowflake environment

Make sure you already create the snowflake account

- ### Create the virtual warehouse

- Go to create > choose sql worksheet
- In sql worksheet write this query to create the warehouse, database, role and schema:

```
-- Set the root role to execute the query
use role accountadmin;

-- Create the warehouse 
create warehouse if not exists dbt_warehouse with warehouse_size='x-small';
```

- ### Create the source and target database

```
-- Create the database
create database if not exists [YOUR SOURCE DATABASE NAME];
create database if not exists [YOUR TARGET DATABASE NAME];
```
- ### Create the role

```
-- Create the role
create role if not exists [YOUR ROLE NAME];
```

- ### Grant the permission
-- Grant the created warehouse to the created role
grant usage on warehouse [YOUR WAREHOUSE NAME] to role [YOUR SNOWFLAKE ROLE];

-- Grant the created role to the account user
grant role dbt_role to user [YOUR ACCOUNT USER NAME];


-- Grant the source database with created role
grant all on database [YOUR SOURCE DATABASE NAME] to role [YOUR SNOWFLAKE ROLE];

-- Grant the target database with created role
grant all on database [YOUR TARGET DATABASE NAME] to role [YOUR SNOWFLAKE ROLE];

- ### Create the schema

```
-- Switch into the created role
use role [YOUR SNOWFLAKE ROLE];

-- Switch into the source database
use database [YOUR SOURCE DATABASE NAME];

-- Create the schema in source database
create schema if not exists [YOUR SOURCE DATABASE NAME].[YOUR SCHEMA NAME];

-- Switch into the target database
use database [YOUR TARGET DATABASE NAME];

-- Create the schema in target database
create schema if not exists [YOUR SOURCE DATABASE NAME].staging;
create schema if not exists [YOUR SOURCE DATABASE NAME].final;
create schema if not exists [YOUR SOURCE DATABASE NAME].snapshots;
```

- ### Setup source database

since the snowflake sample database it can be insert/updating a new record because the permission setting is like that default I created a database to clone the snowflake sample dataset, the reason is because later I wanna do some testing by insert and updating a new record to a data source. so it just for a project

run this query to clone the snowflake sample dataset into the source database
```
-- Switch into source database
USE DATABASE [YOUR SOURCE DATABASE NAME];

-- Clone each snowflake sample dataset table into the specified schema in source database
CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].customer AS
SELECT * FROM snowflake_sample_data.tpch_sf1.customer;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].supplier AS
SELECT * FROM snowflake_sample_data.tpch_sf1.supplier;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].orders AS
SELECT * FROM snowflake_sample_data.tpch_sf1.orders;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].lineitem AS
SELECT * FROM snowflake_sample_data.tpch_sf1.lineitem;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].part AS
SELECT * FROM snowflake_sample_data.tpch_sf1.part;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].partsupp AS
SELECT * FROM snowflake_sample_data.tpch_sf1.partsupp;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].nation AS
SELECT * FROM snowflake_sample_data.tpch_sf1.nation;

CREATE OR REPLACE TABLE [YOUR SCHEMA NAME].region AS
SELECT * FROM snowflake_sample_data.tpch_sf1.region;

```

Check [HERE](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse) for the full documentation about how to setting up the snowflake environment.

---

- ## Initialize Airflow environment with Astronomer

  - ### Install Astro CLI

  This thing used to start running Airflow locally or to manage Astro from your terminal.
  
  ```
  Run the following command to install the latest version of the Astro CLI directly to PATH:
  
  curl -sSL install.astronomer.io | sudo bash -s

  ```
  Check here for the full documentation: [Install Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli/?tab=linux#install-the-astro-cli)

  - ### Initialize Airflow project 

  Create new directory to Initialize the Airflow project

  ```
  mkdir [AIRFLOW_PROJECT_NAME]
  ```

  Initialize the Airflow project with Astro

 ```
 astro dev init
 ```   

After initialize the project there are new directory and other configuration in your directory

Project Contents
================

Your Astro project contains the files and folders, like:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.


Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: [](https://www.astronomer.io/docs/astro/deploy-code/)

- ### Set up the packages
in the _requirements.txt_ set up this [packages]()

- ### Set up the Dockerfile
Set up the [Dockerfile]() with Astro image to build and Airflow project 

- ### Run the Airflow project
Make sure your Docker is running then run this command build an Airflow project with Astro and run it locally

```
astro dev start
```

> [!NOTE] Use "astro dev stop" to stop the container while developing the script

```

```

- ## Create _.env_ file

  Create .env file to store all credential information.
  
  ```
  touch .env
  ``` 

> [!NOTE]
> Ensure that the required tools and packages are installed and the preparations are set up before starting the implementation!

---
---

Alright, let's get started!

# Developing The ELT Scripts

In this project I just did a Load and Transform actually there is no Extraction task because I used the sample dataset in Snowflake, so I just created the function to Load the sample data source to my database in Snowflake then for the transformation I used DBT to managing that and processit using Cosmos packages to run it effectively.

- ## Create LOAD queries
  
  - [Load queries]()
    - This query used to:
      - Truncate all data in staging schema before load a data
      - Create the table in the staging schema
      - then load the data from another database to the staging schema in selected database in Snowflake 

---

- ## Managing data transformations with DBT and Astronomer Cosmos
  
    - ### Setup DBT
        
        - Install DBT 

          ```
          pip install dbt-[YOUR_SELECTED_DATABASE_NAME]
          ``` 
      
          In this project I'm using Snowflake
    
          ```
          pip install dbt-snowflake
          ```
          
        - Initiate DBT project
     
          ```
          Change directory into dags then create a new directory to initialize the DBT project
            
          cd dags && mkdir dbt && cd dbt 
          ```
    
          ```
          Initialize DBT in selected directory

          dbt init
          ``` 
          ```
          Fill the configuration to initialize the DBT project
    
          account:              [SNOWFLAKE ACCOUNT] 
          database:             [SNOWFLAKE DATABASE NAME]
          password:             [SNOWFLAKE ACCOUNT PASSWORD]
          role:                 [SNOWFLAKE WAREHOUSE ROLE]
          schema:               [SNOWFLAKE DATABASE SCHEMA NAME]
          type:                 [DEFAULT TYPE (the default type is snowflake)]
          user:                 [SNOWFLAKE USER NAME]
          warehouse:            [SNOWFLAKE WAREHOUSE NAME]
          threads (1 or more):  [SET TO THE LOWEST VALUE IF YOUR PC SLOW] 
          ```
> [!IMPORTANT] For the "account" you have to fill with your snowflake account identifier with the specified format
> In this project I used this format:  [organization_name]-[account_name] 
> To see where is the organization_name and account_name, check this documentation: [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier)

After initiating the project, a new directory will be created in the directory you initialize, like this: [dbt directory]()

      - Set the materialization strategy and timezone

        Update your dbt_project.yml file inside the DBT project directory to look like this: [dbt_project.yml]()
  
      - Set up the required packages

        Create a packages.yml file inside your DBT project directory and define the required packages: [packages.yml]()

    - ### Build staging layer model
        
        - Create new directory

          **This directory will used to store all staging model configuration**
          ```
          # Change to the "models" directory in your DBT project directory
          cd [YOUR_DBT_PROJECT_DIR_NAME]/models/
          ```
          ```
          # Create "staging" directory
          mkdir staging
          ```

        - Create Jinja configuration files for the source and all staging models
            
            - First, set up the source configuration
            
            - Next, create all the staging models
          
          **Create the source configuration first**, as it is used to reference the selected schema in your data warehouse. Check here for the [complete staging layer models]()
          
          
    - ### Build marts layer model
      
        - Create new directory

          **This directory will used to store all marts model configuration**
                 
          ```
          # Change to the "models" directory in your DBT project directory
          cd [YOUR_DBT_PROJECT_DIR_NAME]/models/
          ```
          ```
          # Create the "marts/core" directory
          mkdir marts; cd marts; mkdir core
          ```

        - Create Jinja configuration files for the core and all marts models
            - First, create all the marts models
            - Next, set up the core models configuration
     
          **The core models configuration is used to create constraints and perform data quality testing.** Check here for the [complete marts layer models]()

    - ### Create Snapshot

      In this project, I used DBT snapshots **to track and store data changes over time**. These snapshots are based on the **Slowly Changing Dimension (SCD) strategy** defined during the data warehouse design. Check here for the [complete snapshot configuration]()

    - ### Test the DBT model

      After building the DBT model, you can test it by running the following DBT commands:

      ```
      dbt debug    # Checks the database connection and the current DBT environment
      ```

      ```
      dbt deps     # Install the DBT packages specified
      ```

      Then, **run these commands sequentially** to compile all the models:
        
      ```
      dbt run      # Compiles all models and loads them into your target database
      ```  
      
      ```
      dbt snapshot # Create the snapshot models and load it into your target database
      ```
      
      ```
      dbt test     # Runs singular/generic tests and creates the constraints in the models
      ```

---
---

# Create DAG

This DAG used to orchestrate and automate the Load and Transformation task with DBT in Airflow using Cosmos package.

Here's the main component of DAG in this proejct

- ## LOAD data function

This function used to connecting the database in Snowflake and execute the LOAD query to dump the data into the staging schema in target database.

- ## DAG main config

When setting up this DAG, remember that the container actually mounts the local directory, so the structure inside the container matches the structure on your local machine. However, the key difference is that the base path in the container starts with /usr/local/airflow/.

For example, if your DBT project is located at ./dags/dbt/elt_with_dbt_snowflake in your local directory, the path inside the container would be /usr/local/airflow/dags/dbt/elt_with_dbt_snowflake.

Here’s how the paths should look inside your DAG:

```
# Define DBT project paths
DBT_PROJECT_PATH = /usr/local/airflow/ + Path to your DBT project in the container
DBT_PROFILES_PATH = /usr/local/airflow/ + Path to the profiles.yml file in the container

```
```
Set the dbt project and profile config

These configurations are essential for Astronomer Cosmos to know how to interact with your DBT setup.

Project Config: Points Cosmos to your DBT project files.
Profile Config: Specifies the DBT profile (with connection settings) and target environment, enabling Cosmos to run DBT commands properly.

# Configure the DBT project path
dbt_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,        # Path to the DBT project (inside the container)
)

# Configure the DBT profile settings
profile_config = ProfileConfig(
    profile_name="[YOUR PROFILE NAME]",       # Replace with the name of your DBT profile
    target_name="[YOUR TARGET ENV NAME]",     # Replace with your target environment name (e.g., dev or prod)
    profiles_yml_filepath=DBT_PROFILES_PATH,  # Path to profiles.yml (inside the container)
)

```

```
# Set up the DAG configuration
with DAG(
    dag_id="example_dag",                       # Unique identifier for this DAG
    schedule_interval="daily",                  # Scheduler interval, e.g., "daily", "monthly", etc.
    start_date=datetime(year, month, day),      # Specify the start date for scheduling tasks
    catchup=False,                              # Skip any missed runs from the past
    max_active_tasks=1,                         # Limit to one active task at a time during pipeline execution
) as dag:

    
    # Define the tasks

    # Placeholder task to mark the start of the DAG
    start = EmptyOperator(task_id="start_dag")  
    
    # Load task to load the data source into the target database
    load_task = PythonOperator(                     # Python operator to called the load_data function
        task_id="load_data_source",                 # Task name 
        python_callable=load_data,                  # Define the Python function for this task
    )
    
    # Transform task to run the data transformation with DBT Cosmos
    transform_task = DbtTaskGroup(
        group_id="transform_data_with_dbt",         # Define the group ID
        project_config=dbt_project_config,          # Pass the project config
        profile_config=profile_config,              # Pass the profile config
        render_config=RenderConfig(                 # Pass the render config                     
            test_behavior=TestBehavior.AFTER_ALL,   # Set the test behavior to run tests after all models are
        )
    )

    ...... 
    You can add another DBT task group for further transformations (if needed)
    ......

    # Placeholder task to mark the end of the DAG
    end = EmptyOperator(task_id="end_dag")       

    # Set the task dependencies and chain tasks in execution order
    start >> load_task >> transform_staging >> transform_marts >> end     
```

# Deploy to Airflow

- ## Setup Snowflake connection

- Login to Airflow webserver
- admin > connections > add connections
- in add conection, fill with this:

```
Connection id = YOUR CONNECTION NAME (MUST BE SAME WITH IN YOUR DAG)
Connection Type = Snowflake
Login = YOUR ACCOUNT USER NAME
Password = YOUR ACCOUNT PASSWORD
Account = YOUR ACCOUNT IDENTIFIER
Warehouse = YOUR TARGET WAREHOUSE NAME
Database = YOUR TARGET DATABASE NAME
Role = YOUR SELECTED ROLE 
```

For the secureness you can use the Private key to establish the connection, since I just develop locally

- ## Run the pipeline
Trigger the DAG 

[image of the pipeline result]

# Deploy to Looker Studio

After the pipeline succesfully, next I created the dashboard based on the created data warehouse for reporting and analytics with looker studio ....

- ## Connect Looker Studio with Snowflake

- Sign in to Google Looker Studio.
- Click +, and then select Data Source.
- Under the Partner Connectors section, select the Snowflake connector (the connector with the Snowflake logo).
- If required, authorize Google Looker Studio to use this community connector.
- Enter the following Snowflake user credentials to connect to Snowflake:
- Username
- Password or private key
- Click Submit.
- Provide the following parameters required to connect to your Snowflake account:
- Account URL
- Role
- Warehouse
- Database
- Schema
- SQL query

> [!Note] The SQL query cannot end with a semicolon.

- Click Connect.
A page containing data source fields is displayed.

- To visualize your data, click Create Report or Explore.

> [!Note] If you have trouble connecting to your Snowflake account, use the following procedure to revoke access, and then try to connect again.

Check this documentation: [Snowflake to Looker Studio](https://other-docs.snowflake.com/en/connectors/google-looker-studio-connector)


Then you can create your dashboard based on the data in the Snowflake


Check Here's to see the dashboard I created based on the Data Warehouse design [Dashboard](https://lookerstudio.google.com/reporting/8cee9a2c-3a16-44fd-98a8-9c530b20c8fa)


# Testing

I conducted several test queries to ensure the pipeline can run succesfully everyday.  Below are the test and their results:

- ## Integration and functional test
In this test I will add a new record and update some record in data source then run the pipeline to see whether the pipeline will run succesfully and fucntion

```
-- Insert new record to customer tabel in data source
INSERT INTO data_source.customer
VALUES(150001,'TESTING NAME', 'TESTING ADDRESS', 1, '12-345-678-9101', 500000, 'BUILDING', 'TESTING COMMENT');

-- Update a record in customer table in data source
UPDATE data_source.customer
SET c_name = 'SCD-TEST-#000150000'
WHERE c_custkey = 150000;
```
Before update

After update


# Final Result

# Conclusion

Well, you’ve reached the end of this guide. In summary, I’ve shared my learning journey in data engineering, focusing on designing a dimensional model for a Data Warehouse and implementing the ELT process with Snowflake, DBT, Airflow and Astronomer Cosmos, based on a case study in the supply chain business. 

**For the full article about this project you can check out my article on Medium here:** [full-story]().

Thank you for joining me on this learning experience. I hope you’ve gained valuable insights that will help you in your own data engineering journey. If you have any questions or need additional information, feel free to reach out. I’m open to any feedback or suggestions you may have.

**You can connect with me on:** 

- [My LinkedIn](www.linkedin.com/in/ricofebrian)
- [My Medium](https://medium.com/@ricofebrian731)

# References


