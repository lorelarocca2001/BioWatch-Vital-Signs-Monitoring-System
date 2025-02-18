import requests
import csv
import pandas as pd
from io import StringIO
import vitaldb
import json
from datetime import datetime, timedelta
import time
import os

# Get Patient ID
patient_id = int(os.getenv("PATIENT_ID", "1"))

# API URL
url = "https://api.vitaldb.net/cases"

# Make a GET request to retrieve the CSV file
response = requests.get(url)

if response.status_code == 200:
    # Read the CSV file content
    csv_file = StringIO(response.text)
    content = csv_file.getvalue()
    
    # Remove BOM (Byte Order Mark) if present
    if content.startswith('\ufeff'):
        content = content[1:]
    
    csv_file = StringIO(content)
    
    # Read CSV using DictReader
    reader = csv.DictReader(csv_file)
    
    # Filter rows where 'caseid' equals 'patient_id'
    filtered_rows = [row for row in reader if int(row.get('caseid')) == patient_id]
    
    # Extract specific columns from filtered data
    filtered_data = [
        {
            'age': row['age'], 
            'sex': row['sex'], 
            'height': row['height'], 
            'weight': row['weight'], 
            'caseid': row['caseid']
        } 
        for row in filtered_rows
    ]
    
    # Create a DataFrame with extracted clinical data
    clinical_df = pd.DataFrame(filtered_data)
    
    # List of vital sign parameters to extract and their originating device 
    track_names = [
        'Solar8000/ART_DBP', 'Solar8000/ART_SBP', 'Solar8000/HR', 
        'Solar8000/BT', 'Solar8000/VENT_RR', 'Solar8000/PLETH_SPO2'
    ]
    
    # Retrieve patient vital sign data
    vf = vitaldb.VitalFile(patient_id, track_names)
    samples = vf.to_pandas(track_names, 2)
    
    # Remove the first and last 25% of the data
    num_rows = len(samples)
    cutoff = int(num_rows * 0.25)
    filtered_samples = samples.iloc[cutoff:num_rows - cutoff]
    
    # Extract clinical values for the case ID
    clinical_values = clinical_df.iloc[0]
    filtered_samples['caseid'] = clinical_values['caseid']
    
    # Add clinical attributes to vital sign data
    for col in ['age', 'sex', 'height', 'weight']:
        filtered_samples[col] = clinical_values[col]
    
    # Rename columns for better readability
    rename_dict = {
        'sex': 'Gender',
        'age': 'Age',
        'weight': 'Weight',
        'height': 'Height',
        'Solar8000/ART_DBP': 'Diastolic Blood Pressure',
        'Solar8000/ART_SBP': 'Systolic Blood Pressure',
        'Solar8000/HR': 'Heart Rate',
        'Solar8000/BT': 'Body Temperature',
        'Solar8000/VENT_RR': 'Respiratory Rate',
        'Solar8000/PLETH_SPO2': 'Oxygen Saturation'
    }
    
    filtered_samples.rename(columns=rename_dict, inplace=True)

    # Reorder columns for clarity
    columns_order = ['caseid', 'Age', 'Gender', 'Height', 'Weight'] + [
        col for col in filtered_samples.columns if col not in ['caseid', 'Age', 'Gender', 'Height', 'Weight']
    ]
    filtered_samples = filtered_samples[columns_order]
    
    # Convert height from cm to meters
    filtered_samples['Height'] = filtered_samples['Height'].astype(float) / 100
    
    # Handle missing values: Fill numerical columns with their mean
    numeric_cols = filtered_samples.select_dtypes(include=['number']).columns
    filtered_samples[numeric_cols] = filtered_samples[numeric_cols].fillna(filtered_samples[numeric_cols].mean())
    
    # Round 'Body Temperature' to one decimal place
    filtered_samples['Body Temperature'] = filtered_samples['Body Temperature'].apply(lambda x: round(x, 1))
    
    # Convert numeric columns to integers (except specific ones)
    for col in filtered_samples.columns:
        if col not in ['Body Temperature', 'Weight', 'Height'] and filtered_samples[col].dtype in ['float64', 'float32']:
            filtered_samples[col] = filtered_samples[col].round(0).astype(int)
    
    # Prepare JSON payload for Fluentd
    fluentd_url = "http://fluentd:9880/patient.vitals"
    data_records = filtered_samples.to_dict(orient='records')
    utc_timestamp = datetime.utcnow()
    
    # Assign timestamp for each record
    for i, record in enumerate(data_records):
        record['timestamp'] = (utc_timestamp + timedelta(seconds=i * 2)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    # Define retry parameters
    max_retries = 5
    retry_delay = 2
    
    # Send data records to Fluentd
    for record in data_records:
        success = False
        attempt = 0
        
        while not success and attempt < max_retries:
            try:
                json_payload = json.dumps(record)
                headers = {'Content-Type': 'application/json'}
                response = requests.post(fluentd_url, data=json_payload, headers=headers)
                
                if response.status_code == 200:
                    print("Data successfully sent to Fluentd")
                    success = True
                else:
                    print(f"Error sending data to Fluentd: {response.status_code}, {response.text}")
                    attempt += 1
                    time.sleep(retry_delay)
            
            except requests.exceptions.RequestException as e:
                print(f"Connection error: {e}")
                attempt += 1
                time.sleep(retry_delay)
        
        if not success:
            print("Failed to send data after multiple attempts.")
            continue
        
        time.sleep(2)


