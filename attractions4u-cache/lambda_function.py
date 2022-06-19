from decimal import Decimal
import json
import redis
import os


r = redis.StrictRedis(host=os.environ["REDIS_HOST"], port=os.environ["REDIS_PORT"], db=0, charset="utf-8", decode_responses=True)
RESPONSE_TO_INVALID = {
    'body': json.dumps('Invalid request!'),
    'statusCode': 400,
    'headers': {"content-type": "application/json"}
}
DEFAULT_TTL = 15


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def lambda_handler(event, context):
    if 'body' not in event:
        return RESPONSE_TO_INVALID

    data = json.loads(event['body'])
    if 'op' not in data or 'keys' not in data:
        return RESPONSE_TO_INVALID

    op = data['op']
    keys = data['keys']
    res = []

    if op == 'get' or op == 'del':
        if not len(keys):
            return RESPONSE_TO_INVALID

        pipe = r.pipeline()
        func = pipe.get if op == 'get' else pipe.delete
        for k in keys:
            func(k)
        res = pipe.execute()
    elif op == 'set':
        if 'values' not in data:
            return RESPONSE_TO_INVALID
        values = data['values']
        if not len(keys) or len(keys) != len(values):
            return RESPONSE_TO_INVALID

        pipe = r.pipeline()
        for k, v in zip(keys, values):
            pipe.set(k, json.dumps(v, cls=DecimalEncoder), DEFAULT_TTL)
        res = pipe.execute()
    else:
        return RESPONSE_TO_INVALID
    
    return {
        'body': json.dumps(res),
        'statusCode': 200,
        'headers': {"content-type": "application/json"}
    }
