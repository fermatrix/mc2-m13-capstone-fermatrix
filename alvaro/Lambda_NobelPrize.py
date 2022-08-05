import json
import requests
import pandas as pd
import boto3

def lambda_handler(event, context):
    event_category = event["queryStringParameters"]["category"]
    event_year = event["queryStringParameters"]["year"]
    url = 'http://api.nobelprize.org/2.1/nobelPrize/{}/{}'

    resultApi = requests.get(url.format(event_category,event_year))
    
    arrId=[]
    arrPor=[]
    resultApi=resultApi.json()
    resultDict=resultApi[0]
    try:
        prueba2=resultDict['laureates']
        dateAwarded=resultDict['dateAwarded']
        for i in range(len(prueba2)):
            arrId.append(prueba2[i]['id'])
            arrPor.append(prueba2[i]['portion'])
        print(arrId)
    except:
        arrId=None
        dateAwarded=None
        arrPor=None
        
    dfNobelPrize = pd.DataFrame(columns=['awardYear','category','categoryFullName','dateAwarded','prizeAmount','prizeAmountAdjusted','id','portion'] )
    
    
    dfNobelPrize=dfNobelPrize.append({
        
                    'id': arrId,
                    'portion': arrPor,
                    'awardYear': resultDict['awardYear'],
                    'category': resultDict['category']['en'],
                    'categoryFullName': resultDict['categoryFullName']['en'],
                    'dateAwarded': dateAwarded,
                    'prizeAmount': resultDict['prizeAmount'],
                    'prizeAmountAdjusted': resultDict['prizeAmountAdjusted']
                    
            }, ignore_index=True)
    
    dfNobelPrize=dfNobelPrize.set_index('id')
    
    
    
    s3=boto3.client('s3')
    
    content=dfNobelPrize.to_parquet()
    s3.put_object(Body=content, Bucket='alvaro-munoz-nobelprice', Key='raw/nobelPrize/'+ event_category +'-'+ str(event_year)  +'.parquet')
        
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }