import boto3
from decimal import Decimal
from elasticsearch import Elasticsearch
import inflect
import json
import nltk
import os
import string
import time
from urllib.parse import quote
import urllib3


client = Elasticsearch(
    os.environ['ES'],
    http_auth=(os.environ['ES_U'], os.environ['ES_K']),
    max_retries=5,
    request_timeout=60000
)
dynamo = boto3.resource('dynamodb')

attractionTable = dynamo.Table("attractions4u-attractions")
pageHistoryTable = dynamo.Table("attractions4u-page-history")
# searchHistoryTable = dynamo.Table("attractions4u-search-history")


response_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers" : "Authorization, Content-Type",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST,GET"
}


YELP_HOST = os.environ['YELP_HOST']
YELP_PATH = os.environ['YELP_PATH']
YELP_SEARCH_TERM = 'restaurant'
SEARCH_LIMIT = 5
example = {
    "rating": 4,
    "price": "$",
    "phone": "+14152520800",
    "id": "E8RJkjfdcwgtyoPMjQgg_Olg",
    "categories": [
        {
        "alias": "coffee",
        "title": "Coffee & Tea"
        }
    ],
    "review_count": 1738,
    "name": "Four Barrel Coffee",
    "location": {
        "city": "San Francisco",
        "country": "US",
        "address2": "",
        "address3": "",
        "state": "CA",
        "address1": "375 Valencia St",
        "zip_code": "94103"
    }
}


yelp_headers = urllib3.make_headers()
yelp_headers['Authorization'] = f"Bearer {os.environ['YELP_K']}"
http = urllib3.PoolManager()


def get_request(host, path, url_params):
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    print(url_params)
    response = http.request('GET', url, headers=yelp_headers, fields=url_params)
    return json.loads(response.data.decode('utf8'))


def search_restaurants(term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': int(SEARCH_LIMIT),
        'radius': int(40000)
    }
    res = get_request(YELP_HOST, YELP_PATH, url_params).get('businesses', [])
    for i in range(len(res)):
        res[i] = {k: v for k, v in res[i].items() if k in example}
    return res


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def get_location_query(addr: str):
    res = addr
    parts = addr.replace('，', ',').replace(', ', ',').split(',')
    if 'USA' in addr:
        res = parts[-2]
    elif 'China' in addr:
        if addr.startswith('China'):
            res = '+'.join([parts[1], parts[-1][-6:], parts[0]])
        else:
            res = '+'.join(parts[-3:])
    elif 'Vietnam' in addr:
        res = '+'.join(parts[-3:])
    else:
        res = '+'.join(parts[-2:])
    return res


NOUN_TAGS = {'NN', 'NNS', 'NNPS', 'NNP'}
INFLECT = inflect.engine()

def proc_usr_query(query_str: str):
    res_tokens = []
    for token, tag in nltk.pos_tag(query_str.replace('_', ' ').translate(str.maketrans('', '', string.punctuation)).split()):
        if tag in NOUN_TAGS:
            singular = INFLECT.singular_noun(token)
            if singular:
                res_tokens.append(singular)
            else:
                res_tokens.append(token)
        else:
            res_tokens.append(token)
    return ' '.join(res_tokens)


def lambda_handler(event, context):
    username = event['requestContext']['authorizer']['jwt']['claims']['email']
    body = ''
    t = time.time_ns()

    path = event['routeKey']
    if path == "GET /search/{keyword}":
        keyword = event['pathParameters']['keyword']
        keywordP = proc_usr_query(keyword)
        # print(f"[DEBUG] USER QUERY BEFORE {keyword}, AFTER {keywordP}")

        query = {
            "size": 60,
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "should": [
                                { "match_phrase": {"attractionTypeP": keywordP} },
                                { "match_phrase": {"attractionName": keyword} },
                                { "match_phrase": {"address": keyword} },
                                { "match_phrase": {"descriptionP": keywordP} },
                                { "match_phrase": {"rekognitionLabels": keywordP} }
                            ]
                        }
                    },
                    "random_score": {
                        "seed": time.time_ns()
                    }
                }
            },
            "fields": ["_id"],
            "_source": False
        }
        response = client.search(index='attractions', body=query)
        ids = [{'attractionId': item['_id']} for item in response["hits"]["hits"]]
        if len(ids):
            # Update search history
            # historyRes = searchHistoryTable.get_item(
            #     Key={
            #         "username": username,
            #         "query": keyword
            #     }
            # )
            # if 'Item' in historyRes:
            #     historyRow = historyRes['Item']
            #     historyRow['cnt'] += 1
            #     historyRow['lastHit'] = t
            # else:
            #     historyRow = {
            #         "username": username,
            #         "query": keyword,
            #         "cnt": 1,
            #         "lastHit": t
            #     }
            # searchHistoryTable.put_item(Item=historyRow)

            # Get attraction details given IDs
            batch_q = {
                attractionTable.name: {
                    'Keys': ids,
                    'ProjectionExpression': "attractionId, attractionName, description, photos, rating, reviews_cnt"
                }
            }
            tmp = dynamo.batch_get_item(RequestItems=batch_q)
            body = tmp['Responses'][attractionTable.name]

        else:
            body = []

    elif path == "GET /attraction/{attractionId}":
        attractionId = event['pathParameters']['attractionId']
        tmp = attractionTable.get_item(Key={'attractionId': attractionId})

        if 'Item' in tmp:
            body = tmp['Item']

            # Update page visit count and missing data
            if 'cnt' in body:
                body['cnt'] += 1
            else:
                body['cnt'] = 1

            if 'weekday_text' not in body['opening_hours']:
                body['opening_hours']['weekday_text'] = []

            attractionTable.put_item(Item=body)

            # Update page view history
            historyRes = pageHistoryTable.get_item(
                Key={
                    "attractionId": attractionId, 
                    "username": username
                }
            )
            if 'Item' in historyRes:
                historyRow = historyRes['Item']
                historyRow['cnt'] += 1
                historyRow['lastVisit'] = t
            else:
                historyRow = {
                    "attractionId": attractionId, 
                    "username": username,
                    "cnt": 1,
                    "lastVisit": t
                }
            pageHistoryTable.put_item(Item=historyRow)

            if 'restaurants' not in body:
                body['restaurants'] = search_restaurants(YELP_SEARCH_TERM, get_location_query(body['address']))
        else:
            return {
                "statusCode": 404,
                'body': json.dumps({"err": f'Attraction with ID {attractionId} doesn\'t exist!'}),
                'headers': response_headers
            }
    else:
        return {
            "statusCode": 400,
            'body': json.dumps({"err": "Unsupported path!"}),
            'headers': response_headers
        }

    return {
        'statusCode': 200,
        'body': json.dumps(body, cls=DecimalEncoder),
        'headers': response_headers
    }
