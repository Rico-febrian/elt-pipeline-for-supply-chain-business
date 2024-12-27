import os
import logging
import snowflake.connector
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, TestBehavior
from dotenv import load_dotenv

#Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables from .env file
load_dotenv('/usr/home/airflow/.env')
logging.info("Load environment variables..")

# Define Snowflake connection variables
sf_user = os.getenv("SF_USER")
sf_account = os.getenv("SF_ACCOUNT")
sf_password = os.getenv("SF_PASSWORD")
sf_warehouse = os.getenv("SF_WAREHOUSE")
sf_database = os.getenv("SF_DATABASE")
sf_stg_schema = os.getenv("SF_STG_SCHEMA")
sf_role = os.getenv("SF_ROLE")

def load_data():
    """
    This function will read each query to loads data from the data source to the staging layer in Snowflake.
    
    raises:
       Exception: If there's an error in connecting to Snowflake or executing SQL queries.
    
    """
    logging.info("Starting LOAD data...")
    
    try:
        
        logging.info("Establishing connection to Snowflake..")
        
        # Define Snowflake connection
        conn = snowflake.connector.connect(
            user=sf_user,
            account=sf_account,
            password=sf_password,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_stg_schema,
            role=sf_role
        )
        
        # Define cursor and read SQL query file 
        cur = conn.cursor()
        dags_folder = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(dags_folder, 'load_query', 'load_data_source.sql')
        
        logging.info(f"Reading SQL file from: {sql_file_path}")
        
        try: 
            
            with open(sql_file_path, 'r') as file:
                sql_query = file.read()

            logging.info("Successfully read SQL file")
    
            # Split the SQL file into individual SQL statements        
            sql_statements = sql_query.split(';')
            
            # Iterate over each SQL statement and execute it
            for statement in sql_statements:
                statement = statement.strip()
                if statement:
                    try:
                        cur.execute(statement)
                        print(f"Executed successfully: {statement}")
                    except Exception as e:
                        print(f"Error while executing: {statement} | Error: {e}")
                        
            logging.info(f"Completed executing {sql_statements} SQL statements")
        
        # Catch the exception if there is an error then close the cursor and database connection
        except FileNotFoundError as e:
            logging.error(f"SQL file not found: {sql_file_path}")
            raise
        
        except Exception as e:
            logging.error(f"Error reading or executing SQL file: {str(e)}")
            raise
        
        finally:
            logging.info("Closing database cursor")
            cur.close()
    
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {str(e)}")
        raise
    
    finally:
        if 'conn' in locals():
            logging.info("Closing Snowflake connection")
            conn.close()
    
    logging.info("LOAD data completed successfully")

# ----------------- ################# -----------------



# ----------------- DAG Configuration -----------------

# Define DBT project paths
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/elt_with_dbt_snowflake"
DBT_PROFILES_PATH = "/usr/local/airflow/dags/dbt/elt_with_dbt_snowflake/profiles.yml"

# Set project config
dbt_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# Set profile config with direct mapping
profile_config = ProfileConfig(
    profile_name="elt_with_dbt_snowflake",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH,
)

# Define the DAG
with DAG(
    dag_id="elt_pipeline_with_dbt",         # Define the DAG ID
    schedule_interval="@daily",             # Set the schedule interval
    start_date=datetime(2024, 12, 16),      # Define the start date
    catchup=False,                          # Set catchup to False to skip any historical runs
    max_active_tasks=1                      # Set the maximum number of active tasks to 1
) as dag:

    logging.info("Initializing DAG: elt_pipeline_with_dbt")
    
    # Define the tasks
    start = EmptyOperator(task_id="start_dag")  # Empty operator task to start the DAG
    
    # Python operator task to load data from the data source
    load_task = PythonOperator(                 
        task_id="load_data_source",
        python_callable=load_data,
    )
    
    # Run DBT tasks using Cosmos package to transform data
    transform_staging = DbtTaskGroup(
        group_id="transform_staging_layer",         # Define the group ID
        project_config=dbt_project_config,          # Pass the project config
        profile_config=profile_config,              # Pass the profile config
        render_config=RenderConfig(                 # Pass the render config
            select=["path:models/staging"]          # Select the specified path to choose which models to run
        )
    )
    
    transform_marts = DbtTaskGroup(
        group_id="transform_marts_layer",           # Define the group ID
        project_config=dbt_project_config,          # Pass the project config
        profile_config=profile_config,              # Pass the profile config
        render_config=RenderConfig(                 # Pass the render config
            select=["path:models/marts/core"],      # Select the specified path to choose which models to run
            test_behavior=TestBehavior.AFTER_ALL,   # Set the test behavior to run tests after all models are
        )
    )
    
    end = EmptyOperator(task_id="end_dag")       # Empty operator task to end the DAG

    # Set the task dependencies
    start >> load_task >> transform_staging >> transform_marts >> end 
    
    logging.info("DAG task succesfully initialized..")
