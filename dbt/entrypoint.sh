#!/bin/bash
set -e

# Listing all files and directories in /usr/src/app for debugging
echo "Listing all files and directories in /usr/src/app:"
find ./ /usr/src/app

# Optionally, explicitly check for the presence of key dbt files
echo "Checking for essential dbt files:"
[[ -f /usr/src/app/dbt_project.yml ]] && echo "Found dbt_project.yml" || echo "dbt_project.yml is missing"
[[ -f /usr/src/app/profiles.yml ]] && echo "Found profiles.yml" || echo "profiles.yml is missing"


# Set environment variables for the INT environment
export DBT_TARGET_ENVIRONMENT=int
export GCP_PROJECT_ID=dbt-analytics-engineer-403622
export DBT_INT_DATASET=dbt_stage
#export GOOGLE_APPLICATION_CREDENTIALS=/path/to/int/service/account/key.json

# Run dbt models
dbt run --profiles-dir /usr/src/app --target $DBT_TARGET_ENVIRONMENT > /usr/src/app/dbt_run_log.txt 2>&1
# Optionally, display the log file for debugging
cat /usr/src/app/dbt_run_log.txt





# Keep the container running (if necessary)
tail -f /dev/null


++++++++++++++++++++++++++++++


To adapt your script for dynamic environment handling (like dev and int) and make it callable from a Flask API running in a container, you'll need to follow a series of steps. The approach involves modifying the bash script to accept environment parameters and then setting up a Flask API endpoint to execute this script dynamically based on the request it receives.

Step 1: Modify Bash Script for Dynamic Environments
Modify your script to accept an environment argument ($1). This argument will determine which environment variables to set (for dev or int).

bash
Copy code
#!/bin/bash
set -e

# First argument is the environment name (dev or int)
ENVIRONMENT=$1

echo "Environment set to: $ENVIRONMENT"

# Listing all files and directories in /usr/src/app for debugging
echo "Listing all files and directories in /usr/src/app:"
find /usr/src/app

# Optionally, explicitly check for the presence of key dbt files
echo "Checking for essential dbt files:"
[[ -f /usr/src/app/dbt_project.yml ]] && echo "Found dbt_project.yml" || echo "dbt_project.yml is missing"
[[ -f /usr/src/app/profiles.yml ]] && echo "Found profiles.yml" || echo "profiles.yml is missing"

# Set environment variables based on the passed argument
if [[ $ENVIRONMENT == "dev" ]]; then
    export DBT_TARGET_ENVIRONMENT=dev
    export GCP_PROJECT_ID=dbt-dev-project-123456
    export DBT_DEV_DATASET=dbt_dev_dataset
    # export GOOGLE_APPLICATION_CREDENTIALS=/path/to/dev/service/account/key.json
elif [[ $ENVIRONMENT == "int" ]]; then
    export DBT_TARGET_ENVIRONMENT=int
    export GCP_PROJECT_ID=dbt-analytics-engineer-403622
    export DBT_INT_DATASET=dbt_stage
    # export GOOGLE_APPLICATION_CREDENTIALS=/path/to/int/service/account/key.json
else
    echo "Invalid environment specified. Exiting."
    exit 1
fi

# Run dbt models
dbt run --profiles-dir /usr/src/app --target $DBT_TARGET_ENVIRONMENT > /usr/src/app/dbt_run_log.txt 2>&1

# Optionally, display the log file for debugging
cat /usr/src/app/dbt_run_log.txt

# Keep the container running (if necessary)
tail -f /dev/null
Step 2: Setting Up Flask API Endpoint
Here is a basic Flask application with an endpoint that dynamically calls the modified script based on the request. Ensure you have Flask installed in your container (pip install Flask).

python
Copy code
from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

@app.route('/run-dbt', methods=['POST'])
def run_dbt():
    # Extract environment from request
    data = request.get_json()
    environment = data.get('environment')

    # Validate environment
    if environment not in ['dev', 'int']:
        return jsonify({'error': 'Invalid environment specified'}), 400

    # Construct the command
    script_path = '/path/to/your/modified/script.sh'
    command = [script_path, environment]

    # Execute the script
    try:
        output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        return jsonify({'message': 'Success', 'output': output.decode('utf-8')}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'Script execution failed', 'details': e.output.decode('utf-8')}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
This Flask application defines an endpoint /run-dbt that expects a JSON payload with an environment key. It then calls the script with the environment parameter, which sets up the DBT environment and runs the models accordingly.

Deployment Considerations
Ensure the script is executable (chmod +x script.sh) and accessible to the Flask application.
Adjust file paths and environment variable settings according to your actual setup.
Make sure your Flask container has the necessary permissions to execute DBT commands and access GCP resources.
Depending on your container environment, you might need to configure additional security or network settings to allow the Flask API to function correctly.
User
in url itself, if I can pass int or dev, so that can hit specific URL & run particular code
ChatGPT
If you prefer to trigger the DBT script execution by passing the environment directly in the URL (e.g., /run-dbt/int or /run-dbt/dev), you can modify the Flask API to use dynamic URL paths to determine the environment. This approach simplifies the API call, as the client does not need to send JSON data in the request body.

Here's how you can adjust your Flask application to handle environments via URL paths:


from flask import Flask, jsonify
import subprocess

app = Flask(__name__)

@app.route('/run-dbt/<environment>', methods=['GET'])
def run_dbt(environment):
    # Validate environment
    if environment not in ['dev', 'int']:
        return jsonify({'error': 'Invalid environment specified'}), 400

    # Construct the command
    script_path = '/path/to/your/modified/script.sh'
    command = [script_path, environment]

    # Execute the script
    try:
        output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        return jsonify({'message': 'DBT script executed successfully', 'output': output.decode('utf-8')}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'Script execution failed', 'details': e.output.decode('utf-8')}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
How It Works:
This version of the API defines a dynamic route /run-dbt/<environment>, where <environment> is a variable part of the URL path that can be int or dev.
When you access the API via a GET request to either /run-dbt/dev or /run-dbt/int, the Flask application captures the environment from the URL, validates it, and then proceeds to execute the DBT script with the given environment.
The script execution and error handling remain the same as in the previous example.
Usage:
To trigger the DBT script for the dev environment, you would send a GET request to http://<your-flask-app-host>/run-dbt/dev. To do the same for the int environment, you would use http://<your-flask-app-host>/run-dbt/int.

Deployment Considerations:
Ensure that your Flask application and the script are correctly configured and that the script is accessible and executable within your container.
As before, make sure your Flask container has the necessary permissions to execute DBT commands and access any required resources.
This approach is straightforward and effective for simple use cases. However, for more complex scenarios involving multiple parameters or security concerns, consider using POST requests with JSON payloads or implementing additional authentication and authorization mechanisms.
