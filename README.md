# Project ALPlanet

## Introduction

This project implements an ETL pipeline that reads a CSV file, processes the data, and stores it in a PostgreSQL database.

## Directory Structure

Project_ALPlanet/
 .venv/
 Config/
   config_template.json
   local_config_template.json
data/
   AB_NYC_2019.csv
log/
  etl_pipeline.log
  log_file.log
Src/
  .metaflow/
  main.py
  transformation_on_local.py
README.md
requirements.txt


## Setup

### 1. Clone the Repository

sh
git clone https://github.com/satwantsingh007/ETL_Pipeline_Project.git
cd Project_ALPlanet

2. Set Up Virtual Environment
Create and activate a virtual environment:

python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate

3. Install Dependencies
Install the necessary Python packages:

pip install -r requirements.txt

4. Configuration Setup
Copy Template Files
Copy the provided template configuration files and rename them:

cp Config/config_template.json Config/config.json
cp Config/local_config_template.json Config/local_config.json

Edit Configuration Files
Open the Config/config.json and Config/local_config.json files and replace the placeholder values with your actual configuration details.

5. Run the Project
Execute the ETL script:

python Src/main.py


