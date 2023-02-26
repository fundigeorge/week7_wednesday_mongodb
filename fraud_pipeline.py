import pandas as pd
import pymongo
import numpy as np
import zlib
import logging
from pymongo import InsertOne

# Extraction function
def extract_data(call_logs, bill_systems):
    # Load call log data from CSV file
    call_logs = pd.read_csv(call_logs)

    # Load billing data from CSV file
    billing_data = pd.read_csv(bill_systems)

    # Merge the two datasets based on common columns. 
    '''
    there is missing links between call logs and billing system. the data type provided is ambigious
    ASSUMPTION: 
    1. customer_id on billing system corresponds to calling_number
    2. there 1 entry per day for a customer
    '''
   
    merged_data = call_logs.merge(billing_data, how='left', left_on=['customer_id', 'call_date'], right_on=['customer_id', 'transaction_date'])
      
    # Convert call duration to minutes for easier analysis
    merged_data['duration_minutes'] = merged_data['call_duration'] / 60
     
    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data extraction completed.")

    return merged_data

# Transformation function
def transform_data(df:pd.DataFrame):
    # Data cleaning and handling missing values
    # drop missing values
    transformed_data = df.dropna(axis=0, how='any')
    #drop duplicated column as a result of merge
    transformed_data = transformed_data.drop(columns=['transaction_date',])

    # Group and aggregate the data
    # get the average ksh_per_min cost per customer within a period
    average_call_cost = transformed_data.groupby(['caller_number','customer_id']).agg({'duration_minutes':[sum],'transaction_amount':[sum]}).reset_index().droplevel(level=1, axis=1)

    # Identify patterns in the data
    # ...
    average_call_cost['ksh_per_min'] =average_call_cost['transaction_amount']/ average_call_cost['duration_minutes']
 

    #find outlier for the cost per min
    q1 = np.percentile(average_call_cost['ksh_per_min'], 25)
    q3 = np.percentile(average_call_cost['ksh_per_min'], 75)
    iqr = q3-q1
    lower_bound = q1 - 1.5*iqr
    upper_bound = q1 + 1.5*iqr
    average_call_cost['call_activity'] = 'OK'
    average_call_cost.loc[(average_call_cost['ksh_per_min'] > upper_bound) | (average_call_cost['ksh_per_min'] <lower_bound), 'call_activity'] = 'Suspicion'
    
    # Use data compression techniques to optimize performance
    # ...
    compressed_data = zlib.compress(str(average_call_cost.to_dict('records')).encode('utf-8'))
    #compressed_data= average_call_cost.to_dict('records')
    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data extraction completed.")   
    
    return compressed_data

# Loading function
def load_data(data):
    # Connect to MongoDB docker container
    client = pymongo.MongoClient("mongodb://localhost:27071/")
    db = client['customer_billing']    
    collection = db['frauds']
  
    # Create indexes on the collection
    # ...
    collection.create_index([('caller_number', 1)])

    # Use bulk inserts to optimize performance
    bulk_operations = []
    for doc in data:
        bulk_operations.append(InsertOne(doc) )
    print(bulk_operations)
    collection.bulk_write(bulk_operations)

    # Use the write concern option to ensure that data is written to disk
    #db.getMongo().setWriteConcern( { w: 1, j:True, wtimeout: 1000 } )

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data loading completed.")

# Example usage
if __name__ == '__main__':
    call_logs_path = '/home/fundi/moringaschool/week7/mongodb/call_logs.csv'
    billing_systems_path = '/home/fundi/moringaschool/week7/mongodb/billing_systems.csv'
    data = extract_data(call_logs_path, billing_systems_path)
    transformed_data = transform_data(data)
    print(transformed_data)
    load_data(transformed_data)
