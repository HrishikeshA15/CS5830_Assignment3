# Big Data Lab - Assignment 2

## Task 1: DataFetch Pipeline

### Overview:
The DataFetch Pipeline is designed to fetch data files from a specific URL, randomly select a specified number of files, download them, zip them into an archive, and move the archive to a target location.

### Implementation Details:
- **Fetch Data (get_data):**
  - Fetches the page containing location-wise datasets for a specific year using the curl command.
  - Stores the fetched page as an HTML file.
- **Select Files (select_files):**
  - Selects a specified number of data files randomly from the available list of files parsed from the fetched HTML page.
  - Passes the selected file URLs to the next task for downloading.
- **Get Files (get_files):**
  - Downloads the individual data files from the selected URLs using the curl command.
- **Zip Files (zip_files):**
  - Zips the downloaded data files into an archive using the shutil.make_archive function.
- **Move Archive (move_archive):**
  - Moves the generated archive to a specified target location.

### DAG Configuration:
- DAG ID: Fetch_data
- Owner: admin
- Start Date: 2024-01-01
- Retries: 1

## Task 2: Analytic Pipeline

### Overview:
The Analytic Pipeline performs data analytics and visualization on the fetched data files. It waits for the archive to be available, unzips the archive, extracts CSV contents, filters data, computes monthly averages, and creates visualizations using geopandas.

### Implementation Details:
- **Wait for Archive (wait_for_archive):**
  - Waits for the archive file to be available before proceeding further.
- **Unzip Archive (unzip_archive):**
  - Unzips the archive containing the data files into a specified directory.
- **Process CSV Files (process_csv_files):**
  - Reads the processed data from CSV files, filters specific fields, and writes the results to a text file.
- **Compute Monthly Averages (compute_monthly_averages):**
  - Computes monthly averages of the specified fields from the processed data and writes the results to a text file.
- **Create Heatmap Visualization (create_heatmap_visualization):**
  - Creates heatmaps visualization using geopandas based on the computed monthly averages.
- **Delete CSV File (delete_csv_file):**
  - Deletes the temporary directory containing CSV files.

### DAG Configuration:
- DAG ID: analytics_pipeline
- Owner: airflow
- Start Date: 2024-01-01
- Retries: 1
- Schedule Interval: Every 1 minute

The code for the project can be found in the dags folder named "AE20B029_Assignment_2.py". The plots for some of the example runs are given in the results plot.
