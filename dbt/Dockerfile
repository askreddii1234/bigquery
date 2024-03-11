# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install dbt
RUN pip install dbt-bigquery

COPY my_dbt_project .
# Copy the entrypoint script into the container at /usr/local/bin
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
# Copy the pre-made dbt project directory into the container


# Copy your profiles.yml to the container
# Adjust the source path if your profiles.yml is in a different location
COPY profiles.yml /usr/src/app/



# Give execution rights on the entrypoint script
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entrypoint to your script. This will run when the container starts.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Keep the container running (if necessary)
#tail -f /dev/null
