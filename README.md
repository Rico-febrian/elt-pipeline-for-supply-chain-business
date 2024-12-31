[!Title Image]()

# Overview

**Hi there! Welcome to my learning logs**.

In this guide, I will share how I developed an ELT pipeline based on a designed dimensional model for a supply chain business using a case study.

---
---

# Objective

**In this repository, I’ll focus specifically on how I developed the ELT pipeline, including:**

- Developing the ELT scripts
  
- Building data transformation models using DBT
  
- Orchestrating and automating the pipeline with Airflow and Astronomer
  
- Designing and creating dashboards in Looker Studio

---
---

# Case Study Background

Assume that I have already conducted a requirements gathering session with stakeholders and users, and we have reached a mutual agreement on the solution to be implemented. 

In summary, here is the current state of the company, the problems it faces, and its key needs to provide context:

- ## Business High-priority Metrics/KPI's

  - Sales performance (daily, monthly, yearly).
  - Profitability analysis.
  - Order fulfillment rate.
  - Customer segmentation.
    
- ## Current Problem

  - **Inefficient analysis of critical metrics**

    Stakeholders and users have trouble analyzing important metrics effectively because the data is spread out and hard to work with.

  - **Complex data management**

    All the data is in a single, large database, so lots of joins and aggregations are needed, making reporting time-consuming.

  - **High costs**

    Managing data inefficiently, along with large data volumes, leads to high costs. The growing data is becoming more expensive to manage.

  - **Lack of visualization tools**

    The business currently doesn’t have a good visualization platform to help with effective analytics.

- ## Stakeholders' & Users' Needs

  - Clean, accurate, and reliable data for reporting and analytics.
  - Fast data retrieval and efficient analysis.
  - Daily data updates.
  - A solution that can scale and be cost-efficient as the business grows.

- ## Data Source Condition
  
  - **Data flow**

    The current source data is collected from daily batch file uploads and API feeds.
  
  - **Update frequency**

    The data is updated on a daily basis.
  
  - **Data volume and growth trends**

    - The database currently stores over 8 million rows across several core tables.

    - As the company expands its operations, data volume is expected to grow at a rate of around 100,000–200,000 new rows per month, primarily driven by increasing transaction volumes and a larger supplier and customer network.

- ## Data Quality
  
The current data quality is good, with no missing values. The values are consistent, easy to understand, and suitable for analysis. However, some tables contain duplicate records due to the detailed granularity of information. Despite these issues, the data remains effective for use.

- ## Agreed Solution

  - Design and build a data warehouse based on current data source and high-priority metrics/KPI's.
    
  - Implement ELT pipeline to run daily updates.

  - Create visualizations and dashboards from the data warehouse to support decision-making.

  - Cost-efficiency improvement by using a more structured model for data, allowing the system to scale more affordably.

---
---

# Pipeline Workflow Overview

Before diving into the main discussion, take a look at the image below. This illustrates the workflow I followed to build this project.

Although this project is ELT-based, I only focused on the Load and Transform steps. **I skipped the Extract step because I used a sample dataset from Snowflake**. So, this project is mainly about loading and transforming data.

- ## How the pipeline works
  
  - ### Load Task
    The Load task moves data from the source database to the staging schema in the target database.
  
    - **Staging schema**

      Stores the raw data exactly as it came from the source database.

    - **Final schema**

      Stores transformed data, ready for use in analytics and dashboards.
    
    - **Snapshots schema**

       Maintains historical data for certain tables in the final schema.
  
  - ### Transform Task

    The Transform task applies data transformations according to the data warehouse design. This step is executed using DBT (Data Build Tool) and orchestrated with Airflow’s Cosmos package, which automates and organizes the data processing steps.

    - In this step, raw data from the staging schema is processed and transformed to align with the requirements of the dimensional model.
    - The transformed data is then loaded into the final schema, where it becomes accessible for business analysis and reporting.

- ## Why use this workflow?

  This workflow was designed based on the business requirements provided by stakeholders in the case study. Here’s why this workflow fits the scenario:

  ### Benefits:

  - **Clear data organization**

    Data is split into staging (raw data), final (ready-to-use data), and snapshot (historical data). This makes it easier to manage and understand what’s happening at each step.
  
  - **Easy to use and maintain**

    The setup is simple enough for teams to use and keep running smoothly without needing deep technical skills.
  
  - **Efficient transformations with DBT**

    DBT handles data transformations in the final schema. It’s automated, scalable, and makes processing faster and more consistent.
  
  - **Saves Costs**

    By avoiding extra queries on raw data and using materialized views, this workflow saves on computing resources, which reduces costs.
  
  ### Trade-offs:

  - **Risk of losing data in staging layer**
  
    The staging layer only holds raw data temporarily. If an issue occurs during processing, reloading the data from the source may be necessary

    **Mitigation: Add a simple backup or archive step for raw data.**
  
  - **Slower processing for large data in staging layer**

    Refreshing all data in staging can take time as the dataset grows.
  
    **Mitigation: Use incremental loads to process only new or updated data.**
  
  - **Snapshots can get too big**

    Storing historical data in the snapshot schema might slow down performance as it grows.

    **Mitigation: Archive older data or optimize how you query the snapshot.**

---
---

# Dataset Overview

I used a sample dataset from Snowflake called TPCH_SF1. This dataset related to a supply chain business. For more details about the dataset, check this documentation: [Snowflake sample data documentation](https://docs.snowflake.com/en/user-guide/sample-data-tpch) 

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
    - Airflow with Astronomer
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

- ## Set Up the Project Environment

  Create and activate python environment to isolate project dependencies.
  
  ```
  python -m venv your_project_name         
  source your_project_name/bin/activate    # On Windows: your_project_name\Scripts\activate
  ```

- ## Set Up the Snowflake Environment

  - ### Login and Access the SQL Worksheet

    - **Create a Snowflake Account**

      If you don’t have a Snowflake account yet, sign up [here](https://www.snowflake.com/en/data-cloud/platform/).

    - **Access the SQL Worksheet**

      Once logged in, navigate to the **SQL Worksheet** in your Snowflake account.

    - **Run SQL Commands**

      In the SQL Worksheet, write queries to set up the data warehouse.

  - ### Create Virtual Warehouse
      
    ```
    -- Switch to accountadmin role to execute the query
    USE ROLE accountadmin;
    ```
  
    ```
    -- Create the warehouse
    CREATE WAREHOUSE IF NOT EXISTS [YOUR WAREHOUSE NAME] WITH WAREHOUSE_SIZE=[CHOOSE YOUR SIZE];
    ```

  - ### Create the Source and Target Database

    ```
    -- Create your database
    CREATE DATABASE IF NOT EXISTS [YOUR SOURCE DATABASE NAME]; -- Optional
    CREATE DATABASE IF NOT EXISTS [YOUR TARGET DATABASE NAME];
    ```
    Since the Snowflake sample database doesn’t allow inserting or updating records due to default permissions, **I created a source database because I’ll need to test the pipeline later with direct updates and inserts**.
    
    **If your data source already supports updates and inserts, you don’t need to create a source database**.
    
  - ### Create the Role
  
    ```
    -- Create the role to manage permissions for users 
    CREATE ROLE IF NOT EXISTS [YOUR ROLE NAME];
    ```
  
  - ### Grant Permissions
    
    ```
    -- Grant the created role permission to use the warehouse
    GRANT USAGE ON WAREHOUSE [YOUR WAREHOUSE NAME] TO ROLE [YOUR SNOWFLAKE ROLE];
    ```

    ```
    -- Grant the created role to a specific user, allowing them to perform the actions permitted by that role
    GRANT ROLE [YOUR ROLE NAME] TO USER [YOUR ACCOUNT USER NAME];
    ```
    ```
    -- Grant the created role permission to use the source and target databases
    GRANT ALL ON DATABASE [YOUR SOURCE DATABASE NAME] TO ROLE [YOUR SNOWFLAKE ROLE];
    GRANT ALL ON DATABASE [YOUR TARGET DATABASE NAME] TO ROLE [YOUR SNOWFLAKE ROLE];
    ```

  - ### Create the Schema
    
    ```
    -- Switch into the created role
    USE ROLE [YOUR SNOWFLAKE ROLE];
    ```
    ```
     -- Switch into the source database
    USE DATABASE [YOUR SOURCE DATABASE NAME];
    
    -- Create the schema in source database
    CREATE SCHEMA IF NOT EXISTS [YOUR SOURCE DATABASE NAME].[YOUR SCHEMA NAME];
    ```
    ```
    -- Switch into the target database
    USE DATABASE [YOUR TARGET DATABASE NAME];

    -- Create the schema in target database
    CREATE SCHEMA IF NOT EXISTS [YOUR TARGET DATABASE NAME].staging;
    CREATE SCHEMA IF NOT EXISTS [YOUR TARGET DATABASE NAME].final;
    CREATE SCHEMA IF NOT EXISTS [YOUR TARGET DATABASE NAME].snapshots;
    ```

  - ### Set Up the Data Source
  
    Run this query to clone the Snowflake sample dataset into the source database.
    ```
    -- Switch into source database
    USE DATABASE [YOUR SOURCE DATABASE NAME];
    
    -- Create the table and clone the dataset into the specified schema
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

For more details about how to setting up the snowflake environment, check the documentation: [Snowflake docs](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse)

---
---

- ## Initialize Airflow Project with Astronomer

  - ### Install Astro CLI

    The Astro CLI is used to run Airflow locally or to manage Astronomer from your terminal.

    Run the following command to install the latest version of the Astro CLI directly to your ```PATH```:
    ```  
    curl -sSL install.astronomer.io | sudo bash -s
    ```
    For more details, Check the documentation: [Install Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli/?tab=linux#install-the-astro-cli)

  - ### Initializing the Airflow Project 

    Create a new directory to initialize the Airflow project

    ```
    mkdir [AIRFLOW_PROJECT_NAME]
    ```

    Navigate to the directory and initialize the Airflow project with Astro
 
    ```
    cd [AIRFLOW_PROJECT_NAME]
    ```
    ```
    astro dev init
    ```   

    After initializing the project, a new directory structure with configuration files will be created in your Airflow project directory.

  - ### Run the Airflow Project

    - **Set up the packages**

      In the [requirements.txt]() file, define the necessary packages for your Airflow project. 
    
    - **Set up the Dockerfile**

      Configure the [Dockerfile]() using the Astro image to build and run your Airflow project. 
    
    - **Run the Airflow project**

      Make sure Docker is running, then use the command to build and start your Airflow project locally:
  
      ```
      astro dev start
      ```

    - **Verify and access the Airflow UI**

      After running your Airflow project, the **Webserver** will be accessible at port ```8080```, and **Postgres** at port ```5432```. If these ports are already in use, either stop the existing Docker containers or change the ports in your configuration.
      
      To access the Airflow UI, open your browser and go to: ```http://localhost:8080/```. Use the following credentials to log in:
      
      - Username: ```admin```
      - Password: ```admin```

For more details about the project contents and how to run Airflow locally with Astronomer, check the documentation: [Airflow with Astronomer](https://www.astronomer.io/docs/astro/cli/develop-project)

- ## Create _.env_ file

  Create .env file in your Airflow project directory to store all credential information.
  
  ```
  touch .env
  ```

---
---

> [!NOTE]
> **Ensure that the required tools and packages are installed and the preparations are set up before starting the implementation!**

# Developing the Load and Transform Scripts for ELT

Although this project is ELT-based, I focused only on the Load and Transform steps. There’s no Extraction step because I used a sample dataset from Snowflake.

- ## Create LOAD Queries
  
  For the Load step, I first created a query that will be used to define the function for the load task in the DAG configuration. Check it out [here]()

  This query is used to:

  - Truncate all data in the staging schema before loading new data.

  - Create tables in the staging schema.

  - Load the sample data (which I had previously cloned into the source database) to the staging schema in target database.

---

- ## Managing Data Transformations with DBT

  For the Transform step, I developed the transformation task using DBT. I created and tested several models before defining them in the DAG. Below are the steps:
  
    - ### Initializing the DBT Project
        
        - **Install DBT**

          ```
          pip install dbt-[YOUR SELECTED DATABASE NAME]
          ``` 
      
          In this project I'm using Snowflake
    
          ```
          pip install dbt-snowflake
          ```
          
        - **Initialize the DBT project**

          Change the directory to ```dags/``` then create a new directory to initialize the DBT project
          ```
          cd dags && mkdir [YOUR DBT PROJECT DIRECTORY] && cd [YOUR DBT PROJECT DIRECTORY] 
          ```
          
          Then run the following command to initialize the DBT project
          ```
          dbt init
          ``` 

          Fill the configuration to initialize the DBT project
          ```  
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
          For the ```account``` field, fill in your Snowflake account identifier following this format:

          - ```[organization_name]-[account_name]```
           
          To find the correct ```organization_name``` and ```account_name```, check this documentation: [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier)
          
          After initializing the project, a new directory will be created in the directory where you initialized it, like this: [link]()

  - ### Set Up DBT Configuration 

      - **Set the materialization strategy and timezone**

        Update the ```dbt_project.yml``` file inside the DBT project directory to look like this: [dbt_project.yml]()
  
      - **Set up the required packages**

        Create a ```packages.yml``` file inside your DBT project directory and define the required packages: [packages.yml]()

  - ### Build Staging Layer Model
      
      - **Create a new directory**

        Change directory to ```models/``` within your DBT project directory, and then create a ```staging``` directory. **This directory will be used to store all staging model configuration**

        ```
        cd models && mkdir staging
        ```
        
      - **Create Jinja configuration for source and staging models**
          
        - **Set up the source configuration**

          Start by setting up the source configuration. This is important because it defines the schema in your data warehouse that you'll reference for loading the data.
        
        - **Create the staging models**

          After setting up the source configuration, proceed to create all the staging models. These models will handle the transformation logic for the raw data.
   
        **Make sure to create the source configuration first**, as it will be referenced in your staging models. For a complete overview of the staging layer models, check here: [staging layer]()
          
  - ### Build Marts Layer Model
    
      - **Create a new directory**

        Change directory to ```models/``` within your DBT project directory, and then create a ```marts/core``` directory. **This directory will be used to store all marts model configurations**  
             
        ```
        cd models && mkdir marts && cd marts && mkdir core
        ```

      - **Create Jinja configuration for marts models**

        - **Create the mart models**

          Start by creating all the mart models. These models will implement the transformation logic for your raw data.
        
        - **Set up the core models configuration**

          After creating the mart models, set up the core models configuration. This configuration is used to create constraints and perform some data quality testing.

        For a complete overview of the marts layer models, check here: [marts layer]()

  - ### Create Snapshot Model
 
    In this project, I used DBT snapshots to track and store changes in data over time. These snapshots follow the **Slowly Changing Dimension (SCD) strategy**, as defined in the data warehouse design. This allows the system to capture historical changes and keep the data up-to-date.

    For a complete overview of the snapshot models, check here: [snapshot models]()
  
  - ### Create Constraints

    After creating the model, you can define relationships between tables and add some generic tests to ensure data consistency and accuracy by setting up a DBT constraints configuration.

    In this project, I created a DBT constraints configuration in the marts layer. For more details about the configuration, check here: [link]
  
  - ### Create Data Tests

    I also wrote singular tests to verify specific functional requirements and validate the data. These tests help ensure the reliability of data transformations and outputs. For more details about the configuration, check here: [link].

  - ### Test the DBT Models

    After building the DBT model, you can test it by running the following DBT commands:

    ```
    dbt debug    # Checks the database connection and the current DBT environment
    ```

    ```
    dbt deps     # Install the DBT packages specified
    ```

    Then, **run these commands sequentially** to compile all the models:
      
    ```
    dbt run      # Compiles all models and loads them into the target database
    ```  
    
    ```
    dbt snapshot # Runs the snapshot models and stores them in the target database
    ```
    
    ```
    dbt test     # Runs both singular and generic tests, applying constraints to the models
    ```

---
---

# Create Directed Acyclic Graph (DAG)

The DAG in this project is used to manage and automate the Load and Transformation tasks using Python and DBT, with Airflow to control the workflow, and the Cosmos package to help run the DBT tasks smoothly.

- ## Key Components of the DAG

  - ### LOAD Data Function

    This function is used to connect to the specified Snowflake database and execute the previously created LOAD query to dump the data into the staging schema of the target database.

  - ### DAG Main Configuration

    This configuration is used to set up how the DAG runs and manages the task workflow. Key components in the configuration include:
 
    - **DBT project paths**

      These paths are necessary for configuring the DBT setup in the DAG.

      ```
      DBT_PROJECT_PATH = AIRFLOW CONTAINER WORKING DIRECTORY + Path to your DBT project in the container
      DBT_PROFILES_PATH = AIRFLOW CONTAINER WORKING DIRECTORY + Path to the profiles.yml file in the container
      DBT_PROFILES_PATH = AIRFLOW CONTAINER WORKING DIRECTORY + Path to the snapshot models in the container
      ```
    
    - **DBT project and profile configuration**

      These configurations are essential for Astronomer Cosmos to know how to interact with your DBT setup.

      ```
      dbt_project_config = ProjectConfig(
          dbt_project_path=DBT_PROJECT_PATH,
          snapshots_relative_path=DBT_SNAPSHOT_PATH,
      )
      ```
      ```   
      profile_config = ProfileConfig(
          profile_name="[YOUR PROFILE NAME]",       # Replace with the name of your DBT profile
          target_name="[YOUR TARGET ENV NAME]",     # Replace with your target environment name (e.g., dev or prod)
          profiles_yml_filepath=DBT_PROFILES_PATH,  
      )
      ```
 
    - **DAG setup**
 
      Standard configuration for scheduling and task management settings.
      
      ```
      with DAG(
          dag_id="example_dag",                       # Unique identifier for this DAG
          schedule_interval="daily",                  # Scheduler interval, e.g., "daily", "monthly", etc.
          start_date=datetime(year, month, day),      # Specify the start date for scheduling tasks
          catchup=False,                              # Skip any missed runs from the past
          max_active_tasks=1,                         # Limit to one active task at a time during pipeline execution
      ) as dag:
      ```
 
    - **Defining the tasks**
 
      Each task is responsible for executing a specific step in the pipeline.
 
      ``` 
      # An EmptyOperator to mark the beginning of the DAG execution
      start = EmptyOperator(task_id="start_dag")
      ``` 
      ```
      # Load task to load the data source into the target database
      load_task = PythonOperator(                     # Python operator to called the load_data function
          task_id="load_data_source",                 # Task name 
          python_callable=load_data,                  # Define the Python function for this task
      )
      ```
      ```
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
  
      ```
      ```
      # An EmptyOperator to mark the end of the DAG execution
      end = EmptyOperator(task_id="end_dag")       
      ```
      ```
      # Set the task dependencies and chain tasks in execution order
      start >> load_task >> transform_staging >> transform_marts >> end     
      ```

> [!IMPORTANT]
> When setting up the DAG, keep in mind that although the Airflow container mounts the local directory, the DAG must refer to paths inside the container, not on your local machine.
>
> **Make sure to check the base path inside the container when defining paths in your DAG**. 
>
> To check the exact path in your container, you can log into the container and use the ``pwd`` command in the terminal to confirm the working directory. This ensures you are using the correct path when referencing files inside the container.
>
> For example, if your DBT project is located at ```./dags/dbt``` on your local machine, the corresponding path inside the container will be ```AIRFLOW CONTAINER WORKING DIRECTORY/dags/dbt```

---
---

# Deploy to Airflow

After developing the tasks and creating the DAG, the next step is to deploy them to Airflow to run the pipeline.

- ## Setup Snowflake-Airflow Connection

  Establish a connection between Airflow and Snowflake before running the pipeline.
  
  - **Login to Airflow UI**
    
    Navigate to the Airflow UI, which by default hosted at ```localhost:8080``` or another custom port you may have configured.
    
  - **Create a new connection**

    - In the Airflow UI, go to the Admin tab.

    - Select Connections from the dropdown.

    - Click the + button to Add a New Connection.
  
  - **Fill in the connection details**

    In the form, fill in the required information as follows:

    ```
    - Connection ID    : This must match the connection name used in your DAG code.
    - Connection Type  : Select Snowflake.
    - Login            : Enter your Snowflake Account Username.
    - Password         : Enter your Snowflake Account Password.
    - Account          : Provide your Snowflake Account Identifier.
    - Warehouse        : The target warehouse you want to use in Snowflake.
    - Database         : The target database in Snowflake.
    - Role             : Specify the role that has the necessary permissions to run your queries.
    ```

> [!CAUTION]
> **For enhanced security, consider using a private key for the connection instead of plain text credentials if you're not working in a local environment!.**

- ## Run the Pipeline

  After setting up the connection, you can trigger your DAG to run the pipeline.

  - **Navigate to your DAG**

    Find the DAG you want to run from the list. Its name refer to the ```dag_id``` defined in the script.

  - **Trigger the DAG**

    Click on the play button (▶️) next to your DAG to trigger it. This will start the execution of the tasks as defined in your pipeline.

---
---

# Deploy to Looker Studio

Once the pipeline has successfully completed, you can create a dashboard for reporting and analytics in Looker Studio based on the data in your data warehouse.

- ## Connect Looker Studio with Snowflake

  - **Sign in to Google Looker Studio**

    Navigate to [Google Looker Studio](https://lookerstudio.google.com/overview) and sign in with your Google account.
    
  - **Add data source**

    - Click the + button to add a new data source.
    - Under the Partner Connectors section, select the Snowflake connector.

  - **Authorize access**
    
    If prompted, grant Google Looker Studio permission to use the Snowflake community connector

  - **Enter Snowflake credentials**

    In the credentials section, provide the necessary information to connect to your Snowflake account

  - **Provide Snowflake connection parameters**
    
    Enter the following details for the connection:
 
    ```
    - Account URL  : Your Snowflake account URL (e.g., xy12345.snowflakecomputing.com)
    - Role         : The role used for querying the data in Snowflake.
    - Warehouse    : The Snowflake warehouse to use for queries.
    - Database     : The target database in Snowflake.
    - Schema       : The schema where your data resides.
    - SQL Query    : Custom SQL query to retrieve the specified data.
    ```
  - **Connect to Snowflake**
    
    Click Connect to establish the connection.

  - **Create the report**
    
    To start working with your data, click Create Report or Explore.

For more details, Check this documentation: [Snowflake to Looker Studio](https://other-docs.snowflake.com/en/connectors/google-looker-studio-connector)

- ## Build the Dashboard
  
Once your data is connected to Looker Studio, you can create visualizations and build your reports and dashboards using the data stored in your Snowflake data warehouse. Customize your reports based on your business needs and analytics requirements.

Check out the project dashboard here: [Dashboard](https://lookerstudio.google.com/reporting/8cee9a2c-3a16-44fd-98a8-9c530b20c8fa)

---
---

# Testing

After the pipeline runs successfully, I performed several test queries to ensure it executes correctly every day. Below are the tests and their results:

- ## Integration and functional test
  
In this test, I add a new record and update some existing records in the data source. Then, I re-run the pipeline to check if it executes successfully and functions as expected

```
-- Insert new record to customer tabel in data source
INSERT INTO data_source.customer
VALUES(150001,'TESTING NAME', 'TESTING ADDRESS', 1, '12-345-678-9101', 500000, 'BUILDING', 'TESTING COMMENT');
```
```
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


