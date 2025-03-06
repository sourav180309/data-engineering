import base64
import boto3
import os
from simple_salesforce import Salesforce

# Salesforce credentials
sfdc_username = 'salesforce_creds1'
sfdc_password = 'salesforce_creds2'
sfdc_security_token = 'your_salesforce_security_token'

# AWS S3 bucket details
s3_bucket_name = 'your_s3_bucket_name'
s3_folder_path = 'salesdata_stage/sfdc/hiredscore/'

# Initialize Salesforce connection
sf = Salesforce(username=sfdc_username, password=sfdc_password, security_token=sfdc_security_token)

# Initialize S3 client
s3 = boto3.client('s3')

# Function to upload file to S3
def upload_to_s3(file_name, file_content):
    s3.put_object(Bucket=s3_bucket_name, Key=file_name, Body=file_content)

# Function to retrieve and upload Salesforce objects and attachments
def transfer_salesforce_data():
    # List of Salesforce objects to be copied
    objects_to_copy = ['Account', 'Contact', 'Opportunity', 'Lead']  # Object names or add a parameter for 

    for obj in objects_to_copy:
        # Query to retrieve data from Salesforce objects
        query = f"SELECT Id FROM {obj}"
        records = sf.query_all(query)['records']

        for record in records:
            record_id = record['Id']

            # Query to retrieve attachments for the current record
            attachment_query = f"SELECT Id, Name, Body FROM Attachment WHERE ParentId = '{record_id}'"
            attachments = sf.query_all(attachment_query)['records']

            for attachment in attachments:
                attachment_id = attachment['Id']
                attachment_name = attachment['Name']
                attachment_body = base64.b64decode(attachment['Body'])

                # Define the S3 file paht
                s3_file_path = os.path.join(s3_folder_path, obj, f"{record_id}_{attachment_name}")

                # Upload attachment to S3
                upload_to_s3(s3_file_path, attachment_body)

    print("Data and attachments have been successfully uploaded to S3.")

# Execute the transfer function
transfer_salesforce_data()
