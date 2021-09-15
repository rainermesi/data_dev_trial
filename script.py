# imports

import constants # for storing credentials
import requests
import boto3
import numpy as np
import pandas as pd
from csv import reader
import time

# Getting to know the Pipedrive API
# Examples are in PHP, but clear, plugging them into py requests was easy
# Wrote 2 funtions: get_data (for searching and retrieving deals) post_data (for uploading deals)

def get_data(searchTerm):
  domain = constants.domain_id 
  token = constants.api_token

  response = requests.get(
      f'https://{domain}.pipedrive.com/api/v1/deals/search?api_token={token}',
      headers={'Authorization': f'Token {token}','Content-Type': 'application/json'},
      params={'term': str(searchTerm)}
  )
  if response.status_code == 200:
      print(response.content)
  else:
      print(f'Got unexpected status code {response.status_code}: {str(response.content)}')

def post_data(payload):
    domain = constants.domain_id
    token = constants.api_token

    response = requests.post(
        f'https://{domain}.pipedrive.com/api/v1/deals?api_token={token}',
        headers={'Authorization': f'Token {token}',
        'Content-Type': 'application/json'
        },
        json = payload
    )
    if response.status_code == 201:
        print('Success :', response.content)
    else:
        print(f'Got unexpected status code {response.status_code}: {str(response.content)}')

# Getting data from S3 to local storage
# Encountered an error, could not progress: 'An error occurred (403) when calling the HeadObject operation: Forbidden'
# Not familiar with boto3, suspect problem with local credential setup.
# Did some reading, considered configuring credentials via AWS Cli, but was spending too much time on this. 
# Decided to move on, ask for feedback.

access_key = constants.aws_access_id
secret_key = constants.aws_secret_id
aws_filename = constants.aws_filename
local_filename = 'dl_deals.csv.gz'
aws_bucket = constants.aws_bucket

def aws_download():
    s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.Bucket(aws_bucket).download_file(local_filename, aws_filename)
    print("Download Successful!")
    return True

aws_download()

# Creating dummy data to complete the task at least to some extent.
# List of generated company names from a Gist I found, coverted into a dataframe.
# Added 1 as person_id (Created a person in UI for myself) and a random number between 100,100000 for value
# Write the file to disk, I imagine the S3 file would have been something similar (but more rows, cols)

def create_dummy_data(url):
    stringList = requests.get(url).text.split('\n')
    df = pd.DataFrame(stringList)
    df[1] = 1
    df.columns=['title','person_id']
    df['title'] = df.apply(lambda x: x['title'] +' Deal',axis=1)
    df['value'] = df.apply(lambda x: np.random.randint(100, 100000),axis=1)
    df.to_csv('deals_output.csv')

create_dummy_data('https://gist.githubusercontent.com/demersdesigns/aac366882659a989e958/raw/47bbfcd36f2829c2686aa7027a75556ccf303fd3/craft-random-company-names')


# Loading the data to Pipedrive
# Opted to read the csv line by line into a payload dictionary
# Added a transformation logic to convert the 'value' value to a float and add 2 decimal spaces
# Sent a create deal command to Pipedrive with post_data()
# I added a timer to estimate how well this approach would perform with more data.
# Runtime for processing 75 rows was 22.913432121276855 seconds
# Thats ~8 hour process for 100k rows

start = time.time()
counter = 0
with open('deals_output.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    header = next(csv_reader)
    if header != None:
        for row in csv_reader:
            counter += 1
            #print(row[3],' : ',str(float(row[3])/100))
            row_payload = {'title':row[1],'person_id' : row[2], 'value': str(float(row[3])/100)}
            post_data(payload=row_payload)
end = time.time()
print(f'Runtime for processing {counter} rows: ', end - start)

# On improving performance
# Use a session object to persist the connection with Pipedrive
# I think at the moment each post_data call creates a new connection.
# Test multiprocessing or other ways to send multiple requests in paralel.

# On updating existing records. 
# My idea is to parse the deals csv line by line (using get_data and post_data): 
# 1 /search for deal by title in Pipedrive (apply filters if possible)
# 2 compare csv and deal properties
# 3 if similar move on, if not update deal from csv data



