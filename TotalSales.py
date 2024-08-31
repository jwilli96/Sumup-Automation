name: Run SumUp Data Script

on:
  schedule:
    - cron: '*/15 * * * *'  # Every 15 minutes
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest

    steps:
      - name: Check out repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Print Directory Structure Before Script Execution
        run: |
          echo "Current working directory before script execution:"
          pwd
          echo "Directory contents before script execution:"
          ls -la

      - name: Install dependencies
        run: |
          set -x
          python -m pip install --upgrade pip
          pip install -r requirements.txt || { echo 'Dependency installation failed'; exit 1; }

      - name: Verify API Key
        run: |
          set -x
          echo "Checking if API key is loaded..."
          if [ -z "$SUMUP_API_KEY" ]; then
            echo "API key is missing or not set correctly."
            exit 1
          else
            echo "API key is loaded successfully."
            echo "API key length: ${#SUMUP_API_KEY}"
          fi
        env:
          SUMUP_API_KEY: ${{ secrets.SUMUP_API_KEY }}

      - name: Run the script
        env:
          SUMUP_API_KEY: ${{ secrets.SUMUP_API_KEY }}
        run: |
          set -x
          python TotalSales.py || { echo 'Script execution failed'; exit 2; }

      - name: Print Directory Structure After Script Execution
        run: |
          echo "Current working directory after script execution:"
          pwd
          echo "Directory contents after script execution:"
          ls -la
          
      - name: List files in 'data' directory
        run: |
          echo "Contents of the data directory (if it exists):"
          ls -la data || echo "'data' directory does not exist."
