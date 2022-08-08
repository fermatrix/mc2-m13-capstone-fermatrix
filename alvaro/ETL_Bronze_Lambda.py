import json
import pandas as pd
import boto3

def lambda_handler(event, context):
    
    df_laureates=pd.read_parquet("s3://alvaro-munoz-nobelprice/raw/laureate")
    df_nobel_prizes=pd.read_parquet("s3://alvaro-munoz-nobelprice/raw/nobelPrizes")
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
    
    
    df_nobel_prizes = pd.read_parquet('nobelPrizes-1901-2019.parquet')

    df_nobel_prizes.dropna(subset = ['dateAwarded'], inplace = True)
    
    df_nobel_prizes=df_nobel_prizes.explode('laureates_id')
    df_nobel_prizes=df_nobel_prizes.explode('laureates_portion')
    
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
    
    mix_df = df_nobel_prizes.join(df_laureates, on='laureates_id', how='inner')

    mix_df=mix_df.rename(columns={'fileName':'file_name'})
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
