import json
import pandas as pd
import boto3

def lambda_handler(event, context):

    df_laureates=pd.read_parquet("s3://alvaro-munoz-nobelprice/raw/laureate")
    df_nobel_prizes=pd.read_parquet("s3://alvaro-munoz-nobelprice/raw/nobelPrize")
    
    new_types={
            "id":"int",
            "fullName":"string",
            "fileName":"string",
            "gender":"string",
            "birth":"string"
        }
        
    
    df_laureates=df_laureates.astype(new_types)
    df_laureates.dropna(subset=['fullName'],inplace=True)
    df_laureates=df_laureates.drop_duplicates(subset=['id','fullName','fileName'])
    df_laureates.sort_index(inplace=True)
        
        
    
    df_nobel_prizes.dropna(subset = ['dateAwarded'], inplace = True)
        
    df_nobel_prizes=df_nobel_prizes.explode('laureates_id', ignore_index=True)
    df_nobel_prizes=df_nobel_prizes.explode('portion')
        
    new_types = {
        'awardYear':'string',
        'category':'string',
        'categoryFullName':'string',
        'dateAwarded':'string',
        'prizeAmount':'int',
        'prizeAmountAdjusted':'int',
        'laureates_id':'int',
    }
        
    df_nobel_prizes=df_nobel_prizes.astype(new_types)
        
    mix_df = df_laureates.join(df_nobel_prizes, on='id', how='inner')
    
    df_bronze_laureates=mix_df.rename(columns={'fileName':'file_name'})
        
        
    s3=boto3.client('s3')
        
    content=df_bronze_laureates.to_parquet()
    s3.put_object(Body=content, Bucket='alvaro-munoz-nobelprice', Key='raw/bronze/bronze_laureates.parquet')
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }