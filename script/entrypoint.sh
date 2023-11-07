#!/bin/bash

# Generate a Fernet key and set it as an environment variable
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")

# Initialize the database and upgrade it
airflow db init
airflow db upgrade

# Create an Airflow user with the "Admin" role (replace placeholders)
if [ -n "$AIRFLOW_ADMIN_USERNAME" ] && [ -n "$AIRFLOW_ADMIN_PASSWORD" ] && [ -n "$AIRFLOW_ADMIN_EMAIL" ] && [ -n "$AIRFLOW_ADMIN_FIRSTNAME" ] && [ -n "$AIRFLOW_ADMIN_LASTNAME" ]; then
  airflow users create -u "$AIRFLOW_ADMIN_USERNAME" -p "$AIRFLOW_ADMIN_PASSWORD" -r Admin -e "$AIRFLOW_ADMIN_EMAIL" -f "$AIRFLOW_ADMIN_FIRSTNAME" -l "$AIRFLOW_ADMIN_LASTNAME"
fi

# Set the Fernet key as an environment variable for Airflow
export AIRFLOW__CORE__FERNET_KEY="$FERNET_KEY"

# Start the Airflow scheduler in the background
airflow scheduler &

# Start the Airflow webserver
exec airflow webserver
