# Import necessary libraries
import pandas as pd
import os
import json
import boto3
from sqlalchemy import create_engine

# List the directory where we will be looking for CSV files to process
directory = r'../data/'
csv_files = [x for x in os.listdir(directory) if x.lower().endswith('.csv')]

if len(csv_files) == 0:
    raise ValueError('no CSV files to process.. aborting')
else:
    print('Found {} CSV files for processing'.format(len(csv_files)))

# Read in all of the data
nv_df = pd.read_csv(directory + '07-07-2023 Nevada Dept of Public _ Behavioral Health.csv', error_bad_lines=False, engine='python')
ok_df = pd.read_csv(directory + '07-07-2023 Oklahoma Human Services.csv', error_bad_lines=False, engine='python')
tx_df = pd.read_csv(directory + '07-07-2023 Texas DHHS.csv', error_bad_lines=False, engine='python')

# Note, I could have used the boto3 library to create an S3 client to read in the files, but I wanted to allot more time for standardization

print('Read in all CSV files')

# Helper functions for dataframe standardization
def drop_unnammed_columns(df):
    '''This function is to help with dropping unnamed columns in CSV files'''
    for col in df.columns:
        if col.lower().startswith('unnamed'):
            df = df.drop(col, axis=1)
            
    return df

def to_date_data_type(df, col_list):
    '''Function for changing data type of columns to date'''

    for col in col_list:
        df[col] = pd.to_datetime(df[col])
        
    return df

def to_numeric_data_type(df, col_list):
    '''Function for changing data type of columns to numeric'''
    
    for col in col_list:
        df[col] = pd.to_numeric(df[col])
        
    return df

def flag_duplicates(df):
    '''Function for flagging duplicate phone numbers and address'''
    
    df['phone'] = df['phone'].str.strip()
    
    for dup in ['phone', 'address1']:
        
        df.loc[nv_df.duplicated(subset=[dup]), 'duplicate_{}'.format(dup)] = 1

    return df

def get_secret_value(secret_id, region):
    '''Function for getting database credentials from Secrets Manager for saving data'''
    secretsmanager_client = boto3.client(
        'secretsmanager',
        region_name=region
    )

    resp = secretsmanager_client.get_secret_value(SecretId=secret_id)
    secret = resp['SecretString']
    secret = json.loads(secret)

    return secret

# First step is standardization of all the dataframes, beginning with Nevada
print('Standarizing Nevada data')
nv_df = drop_unnammed_columns(nv_df)

# Change column names as part of normalization
nv_col_map = {
    'Name': 'company',
    'Credential Type': 'license_type',
    'Credential Number': 'license_number',
    'Status': 'license_status',
    'Expiration Date': 'certificate_expiration_date',
    'Address': 'address1',
    'State': 'state',
    'Phone#': 'phone',
    'First Issue Date': 'license_issued',
    'Primary Contact Name': 'name',
    'Primary Contact Role': 'title'
}

nv_df = nv_df.rename(columns=nv_col_map)

# Extract the zip code from the address
nv_df['zip'] = nv_df['address1'].str.extract(r'(\d+)$')

# Standardize the data types
nv_df['license_number'] = pd.to_numeric(nv_df['license_number'].str.replace('-', ''))

date_cols = ['certificate_expiration_date', 'license_issued']

nv_df = to_date_data_type(nv_df, date_cols)

# Extract first and last name from the names that have 1 person in the names column
nv_df['name_list'] = nv_df['name'].str.split(' ')

nv_df['name_list_len'] = nv_df['name_list'].str.len()

nv_df['first_name'] = nv_df.loc[nv_df['name_list_len'] == 2]['name_list'].str[0]
nv_df['last_name'] = nv_df.loc[nv_df['name_list_len'] == 2]['name_list'].str[1]

# Drop unneeded columns
drop_cols = ['Disciplinary Action', 'County', 'name_list', 'name_list_len']

nv_df = nv_df.drop(columns=drop_cols)

# Check and flag duplicate phone numbers
nv_df = flag_duplicates(nv_df)

# ------------------- Additional steps for standardization with more time -------------------

# Standardize the address by extracting the street number and name into address1, and extracting the city into its own column

# Split the name column into first and last name columns
# I showed how to do a quick parser for easy names (1 person), would need to develop a robust way to extract multiple people into first and last name columns
# -----------------------------------------------------------------------------------------------

# Step 2: Standardize the Oklahoma dataframe
print('Standardizing Oklahoma data')

# Change column names as part of normalization
ok_col_map = {
    'Type License': 'license_type',
    'Company': 'company',
    'Accepts Subsidy': 'accepts_financial_aid',
    'Year Round': 'Year Round',
    'Daytime Hours': 'Daytime Hours',
    'Star Level': 'Star Level',
    'Mon': 'Mon',
    'Tues': 'Tues',
    'Wed': 'Wed',
    'Thurs': 'Thurs',
    'Friday': 'Friday',
    'Saturday': 'Saturday',
    'Sunday': 'Sunday',
    'Primary Caregiver': 'Primary Caregiver',
    'Phone': 'phone',
    'Email': 'email',
    'Address1': 'address1',
    'Address2': 'address2',
    'City': 'city',
    'State': 'state',
    'Zip': 'zip',
    'Subsidy Contract Number': 'Subsidy Contract Number',
    'Total Cap': 'capacity',
    'Ages Accepted 1': 'ages_served_1',
    'AA2': 'ages_served_2',
    'AA3': 'ages_served_3',
    'AA4': 'ages_served_4',
    'License Monitoring Since': 'License Monitoring Since',
    'School Year Only': 'School Year Only',
    'Evening Hours': 'Evening Hours'
}

ok_df = ok_df.rename(columns=ok_col_map)

# Extract the Title and names from the Primary Caregiver column
ok_df['name'] = ok_df['Primary Caregiver'].str.split(r'\r\n').str.get(0)
ok_df['title'] = ok_df['Primary Caregiver'].str.split(r'\r\n').str.get(-1)

ok_df['first_name'] = ok_df['name'].str.split().str.get(0)
ok_df['last_name'] = ok_df['name'].str.split().str.get(1)

# Extract the license type and license number from the license type column
ok_df[['license_type', 'license_number']] = ok_df['license_type'].str.split('-', expand=True)

# Change data types
num_cols = ['capacity']

ok_df = to_numeric_data_type(ok_df, num_cols)

# Drop unneeded columns
drop_cols = ['Year Round', 'Daytime Hours', 'Star Level', 'Subsidy Contract Number', 'License Monitoring Since', 
             'School Year Only', 'Evening Hours', 'Primary Caregiver', 'name']

ok_df = ok_df.drop(columns=drop_cols)

# Flag duplicate phone numbers and addresses
ok_df = flag_duplicates(ok_df)

# ------------------- Additional steps for standardization with more time -------------------

# Combine the Monday-Sunday columns into a schedule column
# I believe the best way to go about this is making the schedule column a dummy variable column
# A value of 1 in that column means that they are open on weekdays only, 2 means weekends only, and 3 means both.

# Format Ages Accepted columns into 2 columns
# First need to find the minimum and maximum age for the ages_served column
# Do this by step through through the Ages Accepted columnns from left to right, seeing which ones contain data
# The leftmost column that contains data can have the min age extracted from there, and the right most column can have the max age extracted from it to get your ages_served column
# Extract the min and max age in the ages_served column to populate the max_age and min_age

# --------------------------------------------------------------------------------------------

# Step 3: Standardize the Texas dataframe
print('Standardizing Texas data')

# Change column names as part of normalization
tx_col_map = {
    'Operation #': 'Operation #',
    'Agency Number': 'Agency Number',
    'Operation/Caregiver Name': 'company',
    'Address': 'address1',
    'City': 'city',
    'State': 'state',
    'Zip': 'zip',
    'County': 'County',
    'Phone': 'phone',
    'Type': 'license_type',
    'Status': 'license_status',
    'Issue Date': 'license_issued',
    'Capacity': 'capacity',
    'Email Address': 'email',
    'Facility ID': 'provider_id',
    'Monitoring Frequency': 'Monitoring Frequency',
    'Infant': 'Infant',
    'Toddler': 'Toddler',
    'Preschool': 'Preschool',
    'School': 'School'
}

tx_df = tx_df.rename(columns=tx_col_map)

# Normalize data types
num_cols = ['capacity']
tx_df = to_numeric_data_type(tx_df, num_cols)
    
date_cols = ['license_issued']
tx_df = to_date_data_type(tx_df, date_cols)

# Drop unneeded columns
drop_cols = ['Operation #', 'Agency Number', 'County', 'Monitoring Frequency']
tx_df = tx_df.drop(columns=drop_cols)

# Flag duplicate phone numbers and addresses
tx_df = flag_duplicates(tx_df)

# ------------------- Additional steps for standardization with more time -------------------

# Figure out the ages for the Infant, Toddler, Preschool, and School columns
# Once you have ages, you can repeat the same process as above with stepping left to right to determine which columns are not null
# Once you have the left most and right most columns that aren't null, you can extract the min and max age of those columns so you can get your ages_served column
# You can then extract from the ages_served column to get the min_age/max_age columns

# --------------------------------------------------------------------------------------------

# After standardizing the dataframes, we need to concatenate them into 1 dataframe before saving to Postgres database
print('Combining dataframes into 1 before saving to database')
df = pd.concat([nv_df, ok_df, tx_df])

# --------------------------------------------------------------------------------------------
# I commmented out the entire saving process because I just wanted to show that this is the process for saving data to a Postgres database

# Here, we could use the boto3 library to access AWS secrets manager to get all database credentials and save to a table named prospective_leads in that databse
# Purposely commented out since we don't have real credentials

# --------------------------------------------------------------------------------------------
# secret = get_secret_value('test_secret_name', region='us-east-1')

# username = secret['username']
# password = secret['password']
# host = secret['host']
# port = secret['port']
# database = secret['engine']

# engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
# df.to_sql('prospective_leads', engine)
print('Saved to database')
