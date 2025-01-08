#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Ensure the script is executed from the project root
cd "$(dirname "$0")"

# Define the venv path in the data-pipeline folder
VENV_PATH="./data-pipeline/.venv"

# Step 0: Check if the virtual environment exists
if [ ! -d "$VENV_PATH" ]; then
  echo "Creating virtual environment in $VENV_PATH..."
  python -m venv "$VENV_PATH"
fi

# Step 1: Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_PATH/Scripts/activate"

# Step 2: Install dependencies
echo "Installing dependencies..."
pip install --no-cache-dir -r ./data-pipeline/requirements.txt

# Step 3: Ensure the database container is running
echo "Checking if the database container is running..."
#0 if ! docker ps | grep -q postgis_db; then
  # echo "Database container not running. Please start it with 'docker-compose up -d'."
  # exit 1
# fi

# Step 4: Wait for the database to be ready
echo "Checking database readiness..."
# until docker exec postgis_db pg_isready -U root -d food-and-the-city > /dev/null 2>&1; do
  # echo "Waiting for the database to be ready..."
  # sleep 2
# done
echo "Database is ready!"

# Step 5: Run the data pipeline script
echo "Running the data pipeline..."
python data-pipeline/data-ingestion-pipeline.py > data-pipeline/pipeline.log 2>&1

# Step 6: Deactivate virtual environment
echo "Deactivating virtual environment..."
deactivate

echo "Data pipeline execution complete!"
