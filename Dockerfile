FROM quay.io/astronomer/astro-runtime:12.6.0

# Enable DBT rich logging
ENV AIRFLOW__COSMOS__ENRICH_LOGGING="True"