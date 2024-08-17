#!/bin/bash

<!-- Change -->
<!-- File Name, Uri and Path. According to your task -->

# Define variables
File_Name="sub-est2023_44.csv"
CSV_URL="https://www2.census.gov/programs-surveys/popest/datasets/2020-2023/cities/totals/$File_Name"
LOCAL_PATH="/home/hdoop/$File_Name"
HDFS_PATH="/user/project/dataset/$File_Name"
HIVE_TABLE="project_data.population_data_2"

# Download the CSV file using the variable
echo " "

if [ ! -f $LOCAL_PATH ]; then
  echo "File does not exist. Downloading..."
  if wget $CSV_URL -O $LOCAL_PATH; then
    echo "Download successful: $LOCAL_PATH"
  else
    echo "Download failed: $CSV_URL"
    exit 1
  fi
else
  echo "File already exists: $LOCAL_PATH"
fi

echo "--------------------------------"

# Put the CSV file into HDFS using the variable
echo "Checking files in hdfs location..."
hadoop fs -ls /user/project/dataset/

# Checking HDFS directory if it doesn't exist
if ! hadoop fs -test -d /user/project/; then
  echo "HDFS directory /user/project/ doesn't exist."
fi

echo "Putting file from local path to hdfs path "
hadoop fs -put $LOCAL_PATH $HDFS_PATH

echo "--------------------------------"

# Create a Hive table using the variable
echo "Creating table in hive"
hive -e "CREATE TABLE IF NOT EXISTS $HIVE_TABLE (SUMLEV STRING,STATE STRING,COUNTY STRING,PLACE STRING,COUSUB STRING,CONCIT STRING,PRIMGEO_FLAG STRING,FUNCSTAT STRING,NAME STRING,STNAME STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;"

if [ $? -eq 0 ]; then
  echo "Table creation successful."
fi

echo "--------------------------------"
# Load data into the Hive table using the variable
echo "Load Data Into table from hdfs path."
hive -e "LOAD DATA INPATH '$HDFS_PATH' INTO TABLE $HIVE_TABLE;"

if [ $? -eq 0 ]; then
  echo "Data loading successful."
fi

echo "--------------------------------"
# Query the Hive table using the variable
echo "Data of file"
hive -e "SELECT * FROM $HIVE_TABLE LIMIT 10;"

echo "--------------------------------"
# Remove the CSV file from HDFS using the variable
echo "Deleting file from hdfs path"
if [ -f "$HDFS_PATH" ]; then
  echo " Deleting.. : hadoop fs -rm -f $HDFS_PATH"
  hadoop fs -rm -f $HDFS_PATH
fi

echo " "
echo " "
echo "***Completed***"