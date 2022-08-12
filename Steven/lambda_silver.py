import json
import pandas as pd
import boto3
import numpy as np
from fractions import Fraction

def lambda_handler(event, context):
    
    df_bronze_laureates = pd.read_parquet("s3://alvaro-munoz-nobelprice/raw/bronze")
    
    #Rename of the columns
    df_bronze_laureates = df_bronze_laureates.rename(columns={'prizeAmount':'prize_amount',
                                                            'awardYear':'year', 
                                                            'laureates_id' : 'laureate_id', 
                                                            'portion' : 'laureate_portion', 
                                                            'categoryFullName' : 'category_full_name', 
                                                            'dateAwarded' : 'date_awarded', 
                                                            'prizeAmountAdjusted' : 'prize_amount_adjusted', 
                                                            'fullName' : 'full_name'})
    
    #Change "laureates_portion" into a float column
    refactor_columns = {
        'laureate_portion' : 'float'
    }
    
    df_bronze_laureates = df_bronze_laureates.astype(refactor_columns)
    
    #Creation of the column: prize_amount_real
    prize_amount_real_col = pd.DataFrame(columns=['prize_real_amount'] )
    prize_amount_real_col['prize_real_amount'] = np.sqrt(df_bronze_laureates['prize_amount'] * df_bronze_laureates['laureate_portion'])
    
    #Adding the new column
    df_bronze_new = df_bronze_laureates.assign(prize_real_amount = prize_amount_real_col)
    
    
    #Creating the file "silver_laureates.parquet" in the bucket S3
    s3=boto3.client('s3')
    
    content=df_bronze_new.to_parquet()
    prueba=df_bronze_new.to_json()
    s3.put_object(Body=content, Bucket='alvaro-munoz-nobelprice', Key='raw/silver/silver_laureates.parquet')
    
    return {
        'statusCode': 200,
        'body': prueba
    }