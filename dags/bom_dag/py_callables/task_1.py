import csv
import os
import re
from airflow.models import Variable


input_directory = '/opt/airflow/data/mock_input_file_system' #references mounted docker volume absolute path
output_directory = '/opt/airflow/data/mock_output_object_storage' #references mounted docker volume absolute path

# Ensure the output directory exists
# os.makedirs(output_directory, exist_ok=True)

new_header = ["station_name", 
              "date", 
              "evapo_transpiration", 
              "rain", 
              "pan_evaporation", 
              "temperature_maximum",
              "temperature_minimum", 
              "relative_humidity_maximum", 
              "relative_humidity_minimum",
              "average_wind_speed", 
              "solar_radiation"
             ]


def is_valid_date(date_str):
    date_pattern = r'\d{2}/\d{2}/\d{4}'
    
    return re.match(date_pattern, date_str) is not None


def modify_csv(input_file, output_file, new_header):
    with open(input_file, 'r', newline='', encoding='utf-8', errors='ignore') as input_csvfile, open(output_file, 'w', newline='') as output_csvfile:
        csvreader = csv.reader(input_csvfile)
        csvwriter = csv.writer(output_csvfile)

        # Skip and write the new header
        next(csvreader)
        csvwriter.writerow(new_header)

        # Copy the remaining rows with a valid date format in column 2
        for row in csvreader:
            if len(row) > 1 and is_valid_date(row[1]):
                csvwriter.writerow(row)


try:
    for root, dirs, files in os.walk(input_directory):
        for file in files:
            if file.endswith(".csv"):
                # Construct the input and output file paths
                input_file = os.path.join(root, file)
                output_file = os.path.join(output_directory, file)

                modify_csv(input_file, output_file, new_header)
    print("<CSV PREPERATION OPERATION SUCCESS>")
except Exception as e:
    print(f"Error: {str(e)}")   



total_weather_station_rows = []
try:
    for filename in os.listdir(output_directory):
        if filename.endswith('.csv'):
            with open(os.path.join(output_directory, filename), 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                data = list(csv_reader)
                # strip headers out
                header = data[0]
                data = data[1:]
                total_weather_station_rows.extend(data)

    output_filename = os.path.join(output_directory, 'total_rows_weather_stations.csv')
    with open(output_filename, 'w', newline='') as output_csv:
        csv_writer = csv.writer(output_csv)
        csv_writer.writerow(new_header)
        csv_writer.writerows(total_weather_station_rows)

    print(f"{len(total_weather_station_rows)} CSV rows have been appended into {output_filename}")
except Exception as e:
    print(f"Error: {str(e)}")