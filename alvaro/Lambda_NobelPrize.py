import json
import requests
import pandas as pd
import boto3

def lambda_handler(event, context):
    event_category = 'lit'
    event_year = 2010
    url = 'http://api.nobelprize.org/2.1/nobelPrize/{}/{}'
    
    
    
    resultApi = requests.get(url.format(event_category,event_year))
    
    resultApi=resultApi.json()
    resultDict=resultApi[0]
    prueba2=resultDict['laureates']
    
    dfNobelPrize = pd.DataFrame(columns=['id','portion','awardYear','category','categoryFullName','dateAwarded','prizeAmount','prizeAmountAdjusted'] )
    
    for i in range(len(prueba2)):
        laureateDict=prueba2[i]
        categoryFullNameDict=resultDict['categoryFullName']
        catogeryDict=resultDict['category']
        
        print(laureateDict['id'])
        dfNobelPrize=dfNobelPrize.append({
    
                'id': laureateDict['id'],
                'portion': laureateDict['portion'],
                'awardYear': resultDict['awardYear'],
                'category': catogeryDict['en'],
                'categoryFullName': categoryFullNameDict['en'],
                'dateAwarded': resultDict['dateAwarded'],
                'prizeAmount': resultDict['prizeAmount'],
                'prizeAmountAdjusted': resultDict['prizeAmountAdjusted']
                
        }, ignore_index=True)
    
    dfNobelPrize=dfNobelPrize.set_index('id')
    
    
    
    s3=boto3.client('s3')
    
    content=dfNobelPrize.to_parquet()
    s3.put_object(Body=content, Bucket='alvaro-munoz-nobelprice', Key='raw/nobelPrize/'+ event_category +'-'+str(event_year)  +'.parquet')
        
    return {
        'statusCode': 200,
        'body': ""
    }
