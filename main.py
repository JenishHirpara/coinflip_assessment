# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

############################################################################################################
# SECTION A: Generating dummy customer and transaction data
############################################################################################################

# import csv
# import json
# import random
# import string
# from datetime import datetime, timedelta
#
#
# # Generate customer data
# def generate_customers(num_customers):
#     customers = []
#     for i in range(num_customers):
#         name = ''.join(random.choices(string.ascii_uppercase, k=10))
#         email = f'{name.lower()}@example.com'
#         phone = f'({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}'
#         address = f'{random.randint(1, 9999)} {random.choice(["Main St", "Oak Ave", "Elm St"])}'
#         city = random.choice(["New York", "Los Angeles", "Chicago"])
#         state = random.choice(["NY", "CA", "IL"])
#         zip_code = f'{random.randint(10000, 99999)}-{random.randint(1000, 9999)}'
#         customers.append({
#             "Name": name,
#             "Email": email,
#             "Phone": phone,
#             "Address": f'{address}, {city}, {state} {zip_code}'
#         })
#     return customers
#
#
# # Generate transaction data
# def generate_transactions(customers, num_transactions):
#     transactions = []
#     for i in range(num_transactions):
#         customer = random.choice(customers)
#         transaction_id = f'TR-{i+1:03}'
#         date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%m/%d/%Y')
#         time = f'{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}'
#         product_id = f'PROD-{random.randint(1, 10):03}'
#         price = round(random.uniform(10, 1000), 2)
#         transactions.append({
#             "TransactionID": transaction_id,
#             "Date": date,
#             "Time": time,
#             "ProductID": product_id,
#             "Price": price,
#             "CustomerEmail": customer["Email"]
#         })
#     # Add 10 duplicate rows
#     for i in range(10):
#         duplicate_row = random.choice(transactions)
#         transactions.append(duplicate_row)
#     return transactions
#
#
# # Write customer data to CSV file
# def write_customer_data(customers, filename):
#     with open(filename, 'w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerow(["Name", "Email", "Phone", "Address"])
#         for customer in customers:
#             writer.writerow([customer["Name"], customer["Email"], customer["Phone"], customer["Address"]])
#
#
# # Write transaction data to JSON file
# def write_transaction_data(transactions, filename):
#     with open(filename, 'w') as file:
#         json.dump(transactions, file)
#
#
# if __name__ == '__main__':
#     num_customers = 100
#     num_transactions = 200
#     customers = generate_customers(num_customers)
#     transactions = generate_transactions(customers, num_transactions)
#     write_customer_data(customers, 'customers.csv')
#     write_transaction_data(transactions, 'transactions.json')


##############################################################################################################
# SECTION B: Extracting customer data from AWS S3 bucket
#############################################################################################################

import pandas as pd
import boto3
from io import StringIO
from credentials import *

aws_bucket = "coinflip-jenish"
aws_file_name = "customers.csv"

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
# 's3' is a key word. create connection to S3 using default config and all buckets within S3

obj = s3.get_object(Bucket=aws_bucket, Key=aws_file_name)
# get object and file (key) from bucket

body = obj['Body']
csv_string = body.read().decode('utf-8')
customer_df = pd.read_csv(StringIO(csv_string), sep=",", header=0)
print("Extracted Customer Dataframe from AWS S3:")
print(customer_df)
print("---------------------------------------------------------------------------")

######################################
# SECTION C: Extracting transaction data from Azure Blob Storage
#####################################
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json

azure_file_name = "transactions.json"

azure_container_name="transactions"
connection_string = constr
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(azure_container_name)
blob_client = container_client.get_blob_client(azure_file_name)
streamdownloader = blob_client.download_blob()

fileReader = json.loads(streamdownloader.readall())

# Load transaction data into a dataframe
transaction_df = pd.DataFrame.from_records(fileReader)
print("Extracted Transaction Dataframe from Azure Blob Storage:")
print(transaction_df)
print("----------------------------------------------------------------------------")


######################################################################################################
# SECTION D: Transformation
####################################################################################################

import pandas as pd

# Merge the two dataframes based on the email column
df = pd.merge(customer_df, transaction_df, on='CustomerEmail')
# print(df)

# Drop duplicate records
df.drop_duplicates(inplace=True)

# Drop records with missing values
df.dropna(inplace=True)
# print(df)
# print(df['Date'])
# print(df['Time'])

# Convert date and time fields to standard format
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.time
# print(df['Date'])
# print(df['Time'])
print("Final dataframe after the transformation: ")
print(df)
print("---------------------------------------------------------------------------")

########################################
# SECTION E: AWS Redshift connection
######################################

# import psycopg2
# con=psycopg2.connect(dbname= 'dev', host='coinflip.987019590515.us-east-2.redshift-serverless.amazonaws.com',
# port= '5439', user= username, password= password)
# cur = con.cursor()
#
# query = "select * from shoes"
# cur.execute(query)
# print(cur.fetchone())
# cur.close()
# # con.commit()
# con.close()

#################################
# SECTION F: Creating table inside AWS Redshift database
################################
import awswrangler as wr
boto3.setup_default_session(region_name="us-east-2")
redshift_con = wr.redshift.connect(connection="redshift_glue")

# existing_records_df = wr.redshift.read_sql_query(sql='select * from shoes', con=redshift_con)
# print(existing_records_df)

output = wr.redshift.to_sql(df=df, con=redshift_con, table='merged', schema='public', mode='upsert', primary_keys=['TransactionID'], use_column_names=True)
extract_df = wr.redshift.read_sql_query(sql='select * from merged', con=redshift_con)
print("Dataframe obtained after loading it to AWS redshift through AWS Glue")
print(extract_df)






