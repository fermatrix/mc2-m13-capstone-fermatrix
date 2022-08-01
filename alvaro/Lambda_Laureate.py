import json
import requests
import pandas as pd
import boto3
def lambda_handler(event, context):

    url = 'http://api.nobelprize.org/2.1/laureates?ID={}'
    
    laureate_id = 5
    
    info_laureate = requests.get(url.format(laureate_id))
    
    resultApi=info_laureate.json()
    
    laureateDict=resultApi['laureates'][0]
    
    laureateDict['id']
    laureateDict['fullName']['en']
    laureateDict['fileName']
    laureateDict['gender']
    laureateDict['birth']['date']
    
    dfParquet=pd.DataFrame(columns=['id','fullName','fileName','gender','birth'])
    
    
    
    dfParquet=dfParquet.append({
    
        'id':laureateDict['id'],
        'fullName':laureateDict['fullName']['en'],
        'fileName':laureateDict['fileName'],
        'gender':laureateDict['gender'],
        'birth':laureateDict['birth']['date']
    
    } ,ignore_index=True)
    



    s3=boto3.client('s3')
    
    content=dfParquet.to_parquet()
    s3.put_object(Body=content, Bucket='alvaro-munoz-nobelprice', Key='raw/laureate/'+ str(laureate_id)  +'.parquet')
    
    return {
        'statusCode': 200,
        'body': 'OK'
    }
