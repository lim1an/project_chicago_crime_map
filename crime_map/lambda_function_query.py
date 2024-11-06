import json
import boto3
import random
import time
import datetime
# need to upload packages
import requests
import mysql.connector


sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """ To get crime report data from Chicago Data Portal 
    and send it to sqs queue for further processing."""
    # Event be like: {'where': "date between '2024-03-01T00:00:00.000' and '2024-04-01T00:00:00.000'"}
    # NEW Event: list of the dictionary
    #event = json.loads(event['Records'][0]['body'])
    url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
    app_token = "kiRPEYKI2IXAwYHcrYcJgXnw9"
    for where_dct in event:
        
        where = where_dct['where']
        #date_id = where_dct['where'][14:24]

        query_params = {
            "$where": where,
            "$limit": 1000,
            "$$app_token": app_token
        } # sample 100 reports for each day

        pause_time = random.uniform(1, 5)
        time.sleep(pause_time)
    
        # Request data from the URL with the given query parameters
        response = requests.get(url, params=query_params)
    
        if response.status_code == 200:
            data = json.loads(response.text) # list of dictionaries
                # belike: [{'id': '13384431', ...}, {}]

            # Connect to the MySQL RDS instance
            rds = boto3.client('rds')
            db = rds.describe_db_instances()['DBInstances'][0]
            endpoint = db['Endpoint']['Address']
            PORT = db['Endpoint']['Port']
            
            # Connect to and put data into the MySQL database
            conn = mysql.connector.connect(
                host=endpoint,
                user='username',
                password='password',
                port=PORT,
                database='final_db'
            )

            # retrieve info from queried data
            for crime in data:
                id = crime.get('id')
                case_number = crime.get('case_number')
                date = crime.get('date')
                primary_type = crime.get('primary_type')
                ward = crime.get('ward')
                latitude = float(crime.get('latitude')) if crime.get('latitude') else None 
                longitude = float(crime.get('longitude')) if crime.get('longitude') else None

                # To MySQL table
                cursor = conn.cursor()
                insert_data_query = '''
                    INSERT INTO crime_reports (id, case_number, date, primary_type, ward, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE case_number = VALUES(case_number), 
                                            date = VALUES(date), 
                                            primary_type = VALUES(primary_type),
                                            ward = VALUES(ward),
                                            latitude = VALUES(latitude),
                                            longitude = VALUES(longitude)
                    '''
                cursor.execute(insert_data_query, (id, case_number, date, primary_type, ward, latitude, longitude))
                conn.commit()
                cursor.close()
                
                # To S3 bucket as JSON
                selected_fields = {
                    'id': id,
                    'case_number': case_number,
                    'date': date,
                    'primary_type': primary_type,
                    'ward': ward,
                    'latitude': latitude,
                    'longitude': longitude
                }

                key = f'crime_reports/{id}.json'
                s3.put_object(Bucket='crime-standard-s3-bucket', Key=key, Body=json.dumps(selected_fields))

                print(f'Crime report with ID {id} processed')
                
                pause_time = random.uniform(1, 5)
                time.sleep(pause_time)

            '''
            # To S3 bucket as parquet
            df = pd.DataFrame(data)
            dask_df = dd.from_pandas(df, npartitions=8)
            parquet_buffer = dask_df.to_parquet(engine='pyarrow', compute=False)
            key = 'crime_reports/' + event_id + '.parquet'
            s3.put_object(Bucket='final-crime-parquet-s3-bucket', Key=key, Body=parquet_buffer)
            print(f'Request event {event_id} processed')
            '''    
            
            return {
                'statusCode': 200,
                'body': 'Data successfully inserted into MySQL table'
            }
        else:
            return {
                'statusCode': response.status_code,
                'body': f'Failed to fetch data. Status code: {response.status_code}'
            }
