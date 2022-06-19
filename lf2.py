import boto3
import json
import os
import urllib3
import uuid

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

sqs = boto3.resource('sqs')
q = sqs.Queue(os.environ['SQS_URL'])

headers = urllib3.make_headers(basic_auth='{}:{}'.format(os.environ['ES_MASTER'], os.environ['ES_MASTER_K']))
headers['Content-Type'] = 'application/json'
http = urllib3.PoolManager()


class SendEmail:
    def __init__(self, restaurants, attr):
        self.restaurants = restaurants
        self.attr = attr
        self.business_ids = []
        self.usr_dict = {}
        self.details = {}
        self.messages = ""
        self.sender = os.environ["SENDER"]
        self.AWS_REGION = "us-east-1"
        self.SUBJECT = "Restaurant Suggestions"
        self.CHARSET = "UTF-8"

    def get_id_cuisine(self):
        for item in self.restaurants["hits"]:
            self.business_ids.append(str(item["_id"]))

        # test
        # self.business_ids.append("j8nnY0ySne_OPxsIWh3pNw")

    def collect_usr_info(self):
        self.usr_dict["cuisine"] = self.attr['cuisine']['StringValue']
        self.usr_dict["date"] = self.attr["date"]["StringValue"]
        self.usr_dict["time"] = self.attr["time"]["StringValue"]
        self.usr_dict["people"] = str(self.attr["people"]["StringValue"])
        self.usr_dict["email"] = self.attr["email"]["StringValue"]

    def query_business_data(self, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource("dynamodb", region_name=self.AWS_REGION)
        table = dynamodb.Table("yelp-restaurants")

        for _id in self.business_ids:
            response = table.query(KeyConditionExpression=Key("id").eq(_id))
            for item in response["Items"]:
                if not isinstance(item, dict):
                    continue
                address = item["address"] if "address" in item else ""
                name = item["name"] if "name" in item else ""
                if _id not in self.details:
                    self.details[_id] = (name, address)

    def pack_message(self):
        recommend_num = 1
        recommend_list = []
        for _id, item in self.details.items():
            if not item:
                continue
            name, address = item[0], item[-1]
            desc = str(recommend_num) + '. ' + name + ', located at ' + address
            recommend_list.append(desc)
            recommend_num += 1

        self.messages = "Hello! Here are my %s restaurant suggestions for %s people, for %s at %s:%s. Enjoy your meal!" \
                        % (self.usr_dict["cuisine"], self.usr_dict["people"], \
                           self.usr_dict["date"], self.usr_dict["time"], ",".join(recommend_list))

    def send_email(self, ses=None):
        if not ses:
            ses = boto3.client('ses', region_name=self.AWS_REGION)
        try:
            response = ses.send_email(
                Destination={
                    "ToAddresses": [
                        self.usr_dict["email"]]
                },
                Message={
                    "Body": {
                        "Text": {
                            "Charset": self.CHARSET,
                            "Data": self.messages
                        }
                    },
                    "Subject": {
                        "Charset": self.CHARSET,
                        "Data": self.SUBJECT
                    }
                },
                Source=self.sender
            )
        except ClientError as e:
            return "ERROR"
        else:
            return "NO_ERROR"

    def save_history(self, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource("dynamodb", region_name=self.AWS_REGION)

        table = dynamodb.Table("HistorySugg")
        save_fields = dict()
        save_fields["email"] = self.usr_dict['email']
        save_fields["message"] = self.messages
        response = table.put_item(Item=save_fields)
        print("history: ", response)
        return response

    def run(self):
        self.get_id_cuisine()
        self.collect_usr_info()
        self.query_business_data()
        self.pack_message()
        response = self.send_email()
        if response == "NO_ERROR":
            self.save_history()

        return response


def lambda_handler(event, context):
    for msg in q.receive_messages(
            VisibilityTimeout=5,
            WaitTimeSeconds=5,
            MessageAttributeNames=['All']
    ):
        attr = msg.message_attributes
        print("POLLED MESSAGE: {}".format(attr))

        # Search restaurants with cuisine
        body = json.dumps({
            "size": 3,
            "query": {
                "function_score": {
                    "query": {
                        "match": {"cuisine": attr['cuisine']['StringValue']}
                    },
                    "random_score": {}
                }
            }
        })
        response = http.request(
            'GET',
            os.environ['ES_URL'],
            headers=headers,
            body=body)

        restaurants = json.loads(response.data.decode('utf8'))['hits']
        # print("attr: ", attr)
        # print("restaurants: ", restaurants)

        if restaurants and attr:
            email_handler = SendEmail(restaurants, attr)
            resp = email_handler.run()
            print("send status: %s" % resp)

            if resp == "NO_ERROR":
                # delete message from SQS
                del_response = q.delete_messages(
                    Entries=[
                        {
                            'Id': str(uuid.uuid4()),
                            'ReceiptHandle': msg.receipt_handle
                        }
                    ]
                )
                print("delete messages status: ", del_response)
