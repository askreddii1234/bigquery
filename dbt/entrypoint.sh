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


++++++++++++

from flask import Flask, jsonify
import subprocess
import os

app = Flask(__name__)

def execute_dbt_command(command, env):
    """
    Execute a dbt command with environment variables, and return a detailed response based on the command's success or failure.
    """
    env_vars = {
        'DBT_TARGET_ENVIRONMENT': env,
        # Set additional environment variables here as needed
    }
    command_to_run = f"dbt {command} --profiles-dir /usr/src/app --target $DBT_TARGET_ENVIRONMENT"

    try:
        output = subprocess.check_output(['/bin/bash', '-c', command_to_run], env={**env_vars, **os.environ}, stderr=subprocess.STDOUT)
        # If the command is successful, return its output with a 200 status code
        return jsonify({"status": "success", "output": output.decode()}), 200
    except subprocess.CalledProcessError as e:
        # Parse the error output to determine the appropriate response
        error_output = e.output.decode()
        
        # Example of customizing the response based on known error patterns
        if "Compilation Error" in error_output:
            status_code = 400  # Bad Request
        elif "Test Failure" in error_output:
            status_code = 422  # Unprocessable Entity
        else:
            status_code = 500  # Internal Server Error for unspecified errors
        
        return jsonify({"status": "error", "error": error_output}), status_code

@app.route('/<command>-dbt/<env>', methods=['GET'])
def handle_dbt_command(command, env):
    # Validate command
    if command not in ["run", "debug", "test", "compile"]:
        return jsonify({"error": "Invalid command"}), 400
    return execute_dbt_command(command, env)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


++++

from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

@app.route('/run-script', methods=['POST'])
def run_script():
    # Get environment from JSON data
    data = request.get_json()
    env = data.get('env', 'int')  # Default to 'int' if not specified

    # Set the environment variable based on the JSON parameter
    env_vars = {
        'DBT_TARGET_ENVIRONMENT': env,
        # Include other environment-specific variables here
    }

    try:
        # Pass the environment variables to the subprocess
        output = subprocess.check_output(['/bin/bash', '-c', 'source /path/to/your/script.sh'], env={**env_vars, **os.environ}, stderr=subprocess.STDOUT)
        return output.decode(), 200
    except subprocess.CalledProcessError as e:
        return e.output.decode(), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

+++++++++++++++++++++++++++++++++++++++++=

Modifying the Shell Script to Accept Arguments
First, let's modify your shell script to accept model names as arguments. You'll append these model names to the dbt run command. Here's how you could adjust your script (let's call it run_dbt_models.sh):

bash
Copy code
#!/bin/bash
set -e

# Listing all files and directories in /usr/src/app for debugging
echo "Listing all files and directories in /usr/src/app:"
find /usr/src/app -print

# Optionally, explicitly check for the presence of key dbt files
echo "Checking for essential dbt files:"
[[ -f /usr/src/app/dbt_project.yml ]] && echo "Found dbt_project.yml" || echo "dbt_project.yml is missing"
[[ -f /usr/src/app/profiles.yml ]] && echo "Found profiles.yml" || echo "profiles.yml is missing"

# Set environment variables for the INT environment
export DBT_TARGET_ENVIRONMENT=int
export GCP_PROJECT_ID=dbt-analytics-engineer-403622
export DBT_INT_DATASET=dbt_stage
#export GOOGLE_APPLICATION_CREDENTIALS=/path/to/int/service/account/key.json

# Run dbt models. If no models are specified, run all models.
if [ "$#" -eq 0 ]; then
    dbt run --profiles-dir /usr/src/app --target $DBT_TARGET_ENVIRONMENT
else
    dbt run --profiles-dir /usr/src/app --target $DBT_TARGET_ENVIRONMENT -m $@
fi

# Optionally, display the log file for debugging
cat /usr/src/app/dbt_run_log.txt
This modified script now checks if any arguments (model names) are passed to it. If arguments are provided, it runs those specific models. Otherwise, it runs all models.

Modifying the Flask Application
Next, update the Flask application to run this script with model names as arguments. Here's the adjusted route in the Flask app:

python
Copy code
from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

@app.route('/run-dbt-model', methods=['GET'])
def run_dbt_model():
    # Retrieve model name(s) from query parameters
    models = request.args.get('models', default="", type=str)  # Default to empty string to run all models if none are specified
    models = models.split(",")  # Assuming models are comma-separated
    
    script_path = "./run_dbt_models.sh"
    
    try:
        # Run the script with models as arguments
        result = subprocess.run([script_path] + models, capture_output=True, text=True, check=True)
        
        return jsonify({"success": True, "message": "DBT model(s) executed successfully", "output": result.stdout}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"success": False, "message": "DBT model execution failed", "error": e.stderr}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
This Flask route now takes a models query parameter, which can be a comma-separated list of model names, and passes them as arguments to your shell script. If no models are specified, the script defaults to running all models.

Usage
To run specific models, you would access the Flask route like this:

php
Copy code
http://<flask_app_host>:<port>/run-dbt-model?models=model1,model2
This setup gives you the flexibility to run specific dbt models through an HTTP interface while keeping the logic of setting environment variables and checking for essential files within your shell script.

User
can also add commands like dbt debug , dbt compile to test the connectivity between dbt & ig Query
ChatGPT
To extend the functionality of your Flask application to support other dbt commands like dbt debug and dbt compile, which can be useful for testing connectivity and preparing SQL from models without executing them, you can modify the application to accept a command type as part of the request. Based on this command type, the application will execute the corresponding dbt operation.

Let's modify the Flask application to include these capabilities. This approach involves:

Accepting a command parameter in the request that specifies which dbt command to run (run, debug, compile, etc.).
Depending on the command, the application will construct and execute the appropriate dbt command.
For simplicity, this example will assume that the models parameter is only applicable for the dbt run and dbt compile commands.
Modified Flask Application
python
Copy code
from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

@app.route('/dbt-operation', methods=['GET'])
def dbt_operation():
    # Retrieve the command type (run, debug, compile)
    command_type = request.args.get('command', default=None, type=str)
    # Retrieve model name(s) for run and compile commands
    models = request.args.get('models', default="", type=str)
    
    if command_type not in ['run', 'debug', 'compile']:
        return jsonify({"success": False, "message": "Unsupported command type"}), 400
    
    script_path = "./dbt_wrapper.sh"
    models_list = models.split(",") if models else []
    
    try:
        # Prepare the command
        if command_type in ['run', 'compile']:
            command = [script_path, command_type] + models_list
        else:
            command = [script_path, command_type]
        
        # Execute the command
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        return jsonify({"success": True, "message": f"DBT {command_type} executed successfully", "output": result.stdout}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"success": False, "message": f"DBT {command_type} execution failed", "error": e.stderr}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
Adjusting the Shell Script (dbt_wrapper.sh)
To handle different dbt commands, you'll need to modify your shell script to accept the command type as its first argument and then process model names for run and compile commands. Here’s an example of how you might adjust your shell script:

bash
Copy code
#!/bin/bash
set -e

command_type=$1
shift  # Remove the first argument, which is the command type

# Common tasks before running any dbt command
echo "Listing all files and directories in /usr/src/app:"
find /usr/src/app -print

echo "Checking for essential dbt files:"
[[ -f /usr/src/app/dbt_project.yml ]] && echo "Found dbt_project.yml" || echo "dbt_project.yml is missing"
[[ -f /usr/src/app/profiles.yml ]] && echo "Found profiles.yml" || echo "profiles.yml is missing"

# Execute the specified dbt command
case $command_type in
  run)
    dbt run --profiles-dir /usr/src/app --target int -m "$@"
    ;;
  compile)
    dbt compile --profiles-dir /usr/src/app --target int -m "$@"
    ;;
  debug)
    dbt debug --profiles-dir /usr/src/app --target int
    ;;
  *)
    echo "Unsupported command type: $command_type"
    exit 1
    ;;
esac
This setup enables your Flask application to execute a wider range of dbt operations, providing a flexible web interface for managing and testing your dbt projects. Remember to call the endpoint with the appropriate parameters, for example:

To run dbt debug: http://<flask_app_host>:<port>/dbt-operation?command=debug
To compile specific models: http://<flask_app_host>:<port>/dbt-operation?command=compile&models=model1,model2
Ensure your shell script is executable and adjust any paths or environment variables according to your specific setup and requirements.
