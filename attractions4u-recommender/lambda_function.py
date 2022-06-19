import boto3
from boto3.dynamodb.conditions import Key
from collections import Counter
from decimal import Decimal
import inflect
import json
import nltk
import os
import random
import time
import urllib3


dynamo = boto3.resource('dynamodb')
attractionTable = dynamo.Table("attractions4u-attractions")
profileTable = dynamo.Table("attractions4u-user-profiles")
pageHistoryTable = dynamo.Table("attractions4u-page-history")

cache_headers = urllib3.make_headers()
cache_headers['Content-Type'] = 'application/json'
es_headers = urllib3.make_headers(basic_auth='{}:{}'.format(os.environ['ES_U'], os.environ['ES_K']))
es_headers['Content-Type'] = 'application/json'
http = urllib3.PoolManager()
CACHE_URL = os.environ['CACHE']
ES_URL = os.environ['ES'] + '/attractions/_search'
ES_URL_MULTISEARCH = os.environ['ES'] + '/_msearch'

NOUN_TAGS = {'NN', 'NNS', 'NNPS', 'NNP'}
INFLECT = inflect.engine()

RECOMMENDATION_RETURN_CNT = 36


def proc_attraction_type(t: str):
    if 'nature_reserves' in t:
        t = 'nature_reserves'
    t = t.replace('accomodation', 'accommodation')

    res_tokens = []
    for token, tag in nltk.pos_tag(t.split('_')):
        if tag in NOUN_TAGS:
            singular = INFLECT.singular_noun(token)
            if singular:
                if singular == 'biergarten':
                    singular += ' beer garden'
                res_tokens.append(singular)
            else:
                res_tokens.append(token)
        else:
            res_tokens.append(token)
    return ' '.join(res_tokens)


def proc_attraction_types(types: list):
    res = []
    for t in types:
        res.append(proc_attraction_type(t))
    return res


def query_table_by_username(table, value):
    filtering_exp = Key('username').eq(value)
    return table.query(KeyConditionExpression=filtering_exp).get('Items')


def get_es_query_body_for_pref(profile: dict):
    query = {
        "size": 100,
        "query": {
            "function_score": {
                "random_score": {
                    "seed": time.time_ns(),
                }
            }
        },
        "fields": ["_id"],
        "_source": False
    }

    countries, types = profile['favCty'], profile['attractions']
    types = proc_attraction_types(types)
    location_conditions = (
        [{ "match_phrase": {"address": country} } for country in countries] + 
        [{ "match_phrase": {"description": country} } for country in countries]
    )
    type_conditions = (
        [{ "match": {"attractionTypeP": t} } for t in types] + 
        [{ "match_phrase": {"attractionName": t} } for t in types] + 
        [{ "match_phrase": {"descriptionP": t} } for t in types]
    )

    if len(location_conditions) and len(type_conditions):
        expr = {
            "bool": {
                "must": [
                    { "bool": { "should": location_conditions } },
                    { "bool": { "should": type_conditions } }
                ]
            },
        }
        query["query"]["function_score"]["query"] = expr
        # print('[DEBUG]', expr)
    elif len(location_conditions) or len(type_conditions):
        expr = {
            "bool": {
                "should": location_conditions or type_conditions
            },
        }
        query["query"]["function_score"]["query"] = expr
        # print('[DEBUG]', expr)
    # print('[DEBUG]', query)
    return query


def es_multi_search(query_bodies: list):
    request_body = ''
    for q in query_bodies:
        request_body += '{"index" : "attractions"} \n'
        request_body += f"{json.dumps(q)} \n"

    es_res = http.request(
        'GET',
        ES_URL_MULTISEARCH,
        headers=es_headers,
        body=request_body
    )
    es_res = json.loads(es_res.data.decode('utf8'))
    # print('[DEBUG]', es_res)

    all_ids = []
    for es_ret in es_res['responses']:
        all_ids.append(frozenset([r['_id'] for r in es_ret['hits']['hits']]))
    return all_ids


def search_by_prefs(profiles: list):
    profile_queries = [get_es_query_body_for_pref(p) for p in profiles]
    return es_multi_search(profile_queries)


def search_by_histories(usrs: list):
    all_relevant_ids = []
    for usr in usrs:
        history = query_table_by_username(pageHistoryTable, usr)
        history.sort(key=lambda row: (-row['lastVisit'], -row['cnt']))
        history_ids = [row['attractionId'] for row in history[:5]]
        es_query_body = {
            "query": {
                "function_score": {
                    "query": { "ids": { "values": history_ids } },
                    "random_score": {
                        "seed": time.time_ns()
                    }
                }
            },
            "fields": ["visSimilar", "descSimilar", "attractionTypeP", "rekognitionLabels"],
            "_source": False
        }
        es_res = http.request(
            'GET',
            ES_URL,
            headers=es_headers,
            body=json.dumps(es_query_body)
        )
        es_res = json.loads(es_res.data.decode('utf8'))["hits"]["hits"]

        relevant_ids = set()
        type_cntr = Counter()
        label_cntr = Counter()
        for ret in es_res:
            fields = ret["fields"]
            relevant_ids.update(fields["visSimilar"])
            relevant_ids.update(fields.get("descSimilar", []))
            type_cntr.update(fields["attractionTypeP"])
            label_cntr.update(fields.get("rekognitionLabels", []))

        # Get attractions of relevant types
        del type_cntr["interesting place"]
        relevant_types = type_cntr.most_common()[:15]
        type_query_body = {
            "size": 200,
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "should": [
                                { "match_phrase": {"attractionTypeP": keywordP} }
                                for (keywordP, _) in relevant_types
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
        type_res = http.request(
            'GET',
            ES_URL,
            headers=es_headers,
            body=json.dumps(type_query_body)
        )
        type_res = json.loads(type_res.data.decode('utf8'))["hits"]["hits"]
        relevant_ids2 = set(r["_id"] for r in type_res)

        # Get attractions of relevant labels
        relevant_labels = label_cntr.most_common()[:15]
        label_query_body = {
            "size": 200,
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "should": [
                                { "match_phrase": {"rekognitionLabels": keywordP} }
                                for (keywordP, _) in relevant_labels
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
        label_res = http.request(
            'GET',
            ES_URL,
            headers=es_headers,
            body=json.dumps(label_query_body)
        )
        label_res = json.loads(label_res.data.decode('utf8'))["hits"]["hits"]
        relevant_ids3 = set(r["_id"] for r in label_res)

        relevant_ids = relevant_ids & (relevant_ids2 | relevant_ids3)

        all_relevant_ids.append(relevant_ids)
    return all_relevant_ids


def batch_query_table(table_name, keys):
    query = {}
    query[table_name] = {"Keys": keys}
    response = dynamo.batch_get_item(
        RequestItems=query
    )
    return response['Responses'][table_name]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def cache_get(keys):
    cache_req = {
        'op': 'get',
        'keys': keys
    }
    cache_res = http.request(
        'POST',
        CACHE_URL,
        headers=cache_headers,
        body=json.dumps(cache_req, cls=DecimalEncoder)
    )
    return json.loads(cache_res.data.decode('utf8'))


def cache_set(keys, values):
    cache_req = {
        'op': 'set',
        'keys': keys,
        'values': values
    }
    cache_res = http.request(
        'POST',
        CACHE_URL,
        headers=cache_headers,
        body=json.dumps(cache_req, cls=DecimalEncoder)
    )
    return json.loads(cache_res.data.decode('utf8'))


def get_recommendations(profiles: list, emails: list):
    res = []
    all_ids_by_pref = search_by_prefs(profiles)
    all_ids_by_history = search_by_histories(emails)

    for ids_by_pref, ids_by_history in zip(all_ids_by_pref, all_ids_by_history):
        ret = []

        # Ensure user pref priority
        ids_by_pref = frozenset(random.sample(ids_by_pref, min(100, len(ids_by_pref))))
        ids_by_history = frozenset(random.sample(ids_by_history, min(30, len(ids_by_history))))

        ids_for_usr = [{'attractionId': aid} for aid in (ids_by_pref | ids_by_history)]
        if len(ids_for_usr):
            if len(ids_for_usr) > 100:
                # DynamoDB batchGet limit is 100
                random.seed(time.time_ns())
                random.shuffle(ids_for_usr)
                ids_for_usr = ids_for_usr[:100] 

            ret = batch_query_table('attractions4u-attractions', ids_for_usr)
            for i in range(len(ret)):
                if 'restaurants' not in ret[i]:
                    ret[i]['restaurants'] = []
                if 'weekday_text' not in ret[i]['opening_hours']:
                    ret[i]['opening_hours']['weekday_text'] = []
        res.append(ret)
    return res


def lambda_handler(event, context):
    if 'requestContext' in event:
        # User asks for recommendation
        email = event['requestContext']['authorizer']['jwt']['claims']['email']

        # Try reading from cache
        cached_usr_res = cache_get([email])[0]
        if cached_usr_res is not None:
            cached_usr_res = json.loads(cached_usr_res)
            random.seed(time.time_ns())
            random.shuffle(cached_usr_res)
            return {
                'statusCode': 200,
                'body': json.dumps(cached_usr_res[:RECOMMENDATION_RETURN_CNT], cls=DecimalEncoder),
                'headers': {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                }
            }

        # Cache miss
        profile = query_table_by_username(profileTable, email)[0]
        # print('[DEBUG] Visitor Profile', profile)
        usr_res = get_recommendations([profile], [email])[0]

        cache_set([email], [usr_res])
        random.seed(time.time_ns())
        random.shuffle(usr_res)
        return {
            'statusCode': 200,
            'body': json.dumps(usr_res[:RECOMMENDATION_RETURN_CNT], cls=DecimalEncoder),
            'headers': {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            }
        }
    else:
        # Periodically triggered by CloudWatch to batch update recommendations for all users
        # print('[DEBUG] Triggered')
        profiles = profileTable.scan()['Items']
        emails = [p['username'] for p in profiles]
        res = get_recommendations(profiles, emails)
        cache_set(emails, res)
        return {
            'statusCode': 200
        }
