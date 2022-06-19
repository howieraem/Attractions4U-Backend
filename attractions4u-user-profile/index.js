const axios = require("axios");
const AWS = require("aws-sdk");

const dynamo = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event, context) => {
  let body, tmp;
  let statusCode = 200;
  const headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers" : "Authorization, Content-Type",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST,GET"
  };
  let requestJSON;

  try {
    switch (event.routeKey) {
      case "GET /profile/{username}":
        tmp = await dynamo
          .get({
            TableName: "attractions4u-user-profiles",
            Key: {
              username: event.pathParameters.username
            }
          })
          .promise();
        if ('Item' in tmp) {
          body = tmp.Item;
        } else {
          statusCode = 404;
          body = {'err': "User doesn\'t exist!"};
        }
        break;
      case "POST /register":
        requestJSON = JSON.parse(event.body);
        tmp = await dynamo
          .get({
            TableName: "attractions4u-user-profiles",
            Key: {
              username: requestJSON.username
            }
          })
          .promise();

        await dynamo
          .put({
            TableName: "attractions4u-user-profiles",
            Item: {
              username: requestJSON.username,
              attractions: requestJSON.attractions,
              favCty: requestJSON.favCty
            }
          })
          .promise();
        body = {'success': true};
        break;
      case "POST /update_profile":
        requestJSON = JSON.parse(event.body);
        tmp = await dynamo
          .get({
            TableName: "attractions4u-user-profiles",
            Key: {
              username: requestJSON.username
            }
          })
          .promise();
        if (!('Item' in tmp)) {
          statusCode = 400;
          body = {'success': false, 'err': "User doesn\'t exist!"};
          break;
        }

        await dynamo
          .put({
            TableName: "attractions4u-user-profiles",
            Item: {
              username: requestJSON.username,
              attractions: requestJSON.attractions,
              favCty: requestJSON.favCty
            }
          })
          .promise();
        body = {'success': true};

        const cache_evict_req = {
          "op": "del",
          "keys": [requestJSON.username]
        };
        await axios.post(process.env.CACHE, cache_evict_req, {
          headers: { "Content-Type": "application/json" }
        });

        break;
      default:
        throw new Error(`Unsupported route: "${event.routeKey}"`);
    }
  } catch (err) {
    statusCode = 400;
    console.log(err);
    body = {'success': false, 'err': err.message};
  } finally {
    body = JSON.stringify(body);
  }

  return {
    statusCode,
    body,
    headers
  };
};