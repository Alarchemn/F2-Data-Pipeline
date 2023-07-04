"""
*************************************************************
Author = Martin Alarcon -- https://github.com/Alarchemn     *
Date = '20/08/2023'                                         *
Description = Lambda function to update database            *
*************************************************************
"""

# THIS FUNCTION IS ON AWS LAMBDA AND ITS TRIGGERED BY S3 EVENT (UPLOAD FILE)


import json
import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')

# ------------------ CONSTANTS ----------------------------
db_files = ['Sprint-Race','Feature-Race','Qualifying-Session','Free-Practice','Sprint-Race-2']

# --------------------- FUNCTIONS -------------------------
def reduce_data(race):
    """
    Reduce the data in the 'race' DataFrame by replacing values in the 'TYPE' column and adding a new 'QUALI TYPE' column.

    Args:
        race (pandas.DataFrame): DataFrame containing the race data.

    Returns:
        None. The 'race' DataFrame is modified in place.
    """
    def replace_ab(type):
        if type == 'Qualifying Group B':
            return 'Group B'
        if type == 'Qualifying Group A':
            return 'Group A'
        else:
            return 'Unique'
    
    # Replace values in 'TYPE' column using the 'replace_ab' function
    race['QUALI TYPE'] = race['TYPE'].apply(replace_ab)
    
    # Replace specific values in 'TYPE' column
    race['TYPE'].replace(['Qualifying Group B', 'Qualifying Group A'],'Qualifying Session',inplace=True)
    race['TYPE'].replace('Sprint Race 1','Sprint Race',inplace=True)
    

def format_data(race):
    """
    Extract specific data from the 'race' DataFrame based on the 'TYPE' column.

    Args:
        race (pandas.DataFrame): DataFrame containing the race data.

    Returns:
        dict: A dictionary containing the extracted data for each race type.
    """
    
    db_dict = {
        'Sprint Race': None,
        'Feature Race': None,
        'Qualifying Session': None,
        'Free Practice': None,
        'Sprint Race 2': None
    }
    
    db_drop = {
        'Sprint Race': ['LAP SET ON','QUALI TYPE'],
        'Feature Race': ['LAP SET ON','QUALI TYPE'],
        'Qualifying Session': ['BEST','LAP'],
        'Free Practice': ['BEST','LAP'],
        'Sprint Race 2': ['LAP SET ON','QUALI TYPE'],
    }

    for key in db_dict:
        # Extract data for each race type
        db_dict[key] = race[race['TYPE'] == key]
        
        if key == 'Free Practice':
            # Drop 'QUALI TYPE' column for 'Free Practice' race type (did not work with de dict)
            db_dict[key] = db_dict[key].drop('QUALI TYPE',axis=1)
        try:
            # Drop additional columns specified in 'db_drop' dictionary (FIA change the metrics, so we need to handle exceptions)
            db_dict[key] = db_dict[key].drop(db_drop[key],axis=1)
        except:
            None
        
    return(db_dict)


# --------------------- LAMBDA -------------------------
def lambda_handler(event, context):
    """
    AWS Lambda function handler for processing events.

    Args:
        event (dict): The event data.
        context (object): The runtime information of the Lambda function.

    Returns:
        None
    """
    
    # TODO implement
    database_s3 = {}
    
    # Get the files to update
    for file in db_files:
        key = f'{file}.csv'
        response = s3.get_object(Bucket='f2-events-db',Key=key)
        status = response['ResponseMetadata']['HTTPStatusCode']
    
        if status == 200:
            database_s3[file] = pd.read_csv(response['Body'])
            print(f'Status - {status}: successful S3 get_object response. Key: {key}')
        else:
            print(f"Status - {status}: unsuccessful S3 get_object response.")
            
    # Get the newly created files for the update
    id_bucket = event['Records'][0]['s3']['bucket']['name']
    id_key = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=id_bucket,Key=id_key)
    status = response['ResponseMetadata']['HTTPStatusCode']
    
    if status == 200:
        file = pd.read_csv(response['Body'])
        print(f'Status - {status}: successful S3 get_object response. Key: {key}')
    else:
        print(f"Status - {status}: unsuccessful S3 get_object response.")
        
    # Apply the transformation functions
    reduce_data(file)
    new_data_db = format_data(file)
    
    # Update (load) the files with the new info
    for key,value in database_s3.items():
        new_data_key = key.replace('-',' ')
        concatenated_db = pd.concat([value,new_data_db[new_data_key]])
        
        csv_buffer = StringIO()
        concatenated_db.to_csv(csv_buffer,index=False)
        
        s3.put_object(
            Body=csv_buffer.getvalue(),
            Bucket='f2-events-db',
            Key=f'{key}.csv')
        print(f'successful S3 put_object response. Key: {key}.csv')
    
    print('ALL OK')