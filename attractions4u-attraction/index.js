const AWS = require("aws-sdk");
const dynamo = new AWS.DynamoDB.DocumentClient();
const { Client } = require('@elastic/elasticsearch');
const client = new Client({
  node: process.env.ES,
  auth: {
    username: process.env.ES_U,
    password: process.env.ES_K
  },
  maxRetries: 5,
  requestTimeout: 60000,
});

const attractionTableName = "attractions4u-attractions";
const pageHistoryTableName = "attractions4u-page-history";
// const searchHistoryTableName = "attractions4u-search-history";

exports.handler = async (event, context) => {
  let body, tmp;
  let statusCode = 200;
  const headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers" : "Authorization, Content-Type",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST,GET"
  };

  const username = event.requestContext.authorizer.jwt.claims.email;

  try {
    switch (event.routeKey) {
      case "GET /search/{keyword}":
        // Search in ElasticSearch
        const query = {
          "index": 'attractions',
          "body": {
            "size": 60,
            "query": {
              "function_score": {
                "query": {
                  "bool": {
                    "should": [
                      { "wildcard": {"attractionType": `${event.pathParameters.keyword}*`} },
                      { "match_phrase": {"attractionName": `${event.pathParameters.keyword}`} },
                      { "match_phrase": {"address": `${event.pathParameters.keyword}`} },
                      { "match_phrase": {"description": `${event.pathParameters.keyword}`} },
                      // { "fuzzy": {"attractionType": event.pathParameters.keyword} },
                      // { "fuzzy": {"attractionName": event.pathParameters.keyword} },
                      // { "fuzzy": {"address": event.pathParameters.keyword} }
                    ]
                  }
                },
                "random_score": {
                  "seed": Date.now()
                }
              }
            },
            "fields": ["_id"],
            "_source": false
          }
        };

        const response = await client.search(query);
        const ids = response.body.hits.hits.map(item => { return {'attractionId': item['_id']}; });
        
        if (ids.length === 0) {
          body = [];
          break;
        }
        
        /*
        // Update search history (only if the search result is non-empty)
        const historyQuery = {
          Key: {
            "username": username,
            "query": event.pathParameters.keyword
          },
          TableName: searchHistoryTableName
        };
        const historyRes = await dynamo.get(historyQuery).promise();
        const t = new Date().toISOString();
        let historyRow;
        if ('Item' in historyRes) {
          historyRow = historyRes.Item;
          historyRow.cnt++;
          historyRow.lastHit = t;
        } else {
          historyRow = {
            "username": username,
            "query": event.pathParameters.keyword,
            "cnt": 1,
            "lastHit": t
          };
        }
        await dynamo
          .put({
            TableName: searchHistoryTableName,
            Item: historyRow
          })
          .promise();
        */  

        // Get details given attraction IDs
        let queryParams = {
          "RequestItems": {},
        };
        queryParams.RequestItems[attractionTableName] = {
          Keys: ids,
          ProjectionExpression: "attractionId, attractionName, description, photos, rating, reviews_cnt"
        };

        tmp = await dynamo
          .batchGet(queryParams)
          .promise();

        body = tmp.Responses[attractionTableName];
        break;
      case "GET /attraction/{attractionId}":
        // Read attraction details given ID
        tmp = await dynamo
          .get({
            TableName: attractionTableName,
            Key: {
              attractionId: event.pathParameters.attractionId
            }
          })
          .promise();

        if ('Item' in tmp) {
          body = tmp.Item;
          
          // Update page visit count and missing data
          if ('cnt' in body) {
            body.cnt++;
          } else {
            body.cnt = 1;
          }
          
          if (!('weekday_text' in body.opening_hours)) {
            body.opening_hours.weekday_text = [];
          }

          await dynamo
            .put({
              TableName: attractionTableName,
              Item: body
            })
            .promise();
          
          // Update page view history
          const historyQuery = {
              Key: {
                "attractionId": event.pathParameters.attractionId, 
                "username": username
              },
              TableName: pageHistoryTableName
          };
          const historyRes = await dynamo.get(historyQuery).promise();
          const t = new Date().toISOString();
          let historyRow;
          if ('Item' in historyRes) {
            historyRow = historyRes.Item;
            historyRow.cnt++;
            historyRow.lastVisit = t;
          } else {
            historyRow = {
              "attractionId": event.pathParameters.attractionId, 
              "username": username,
              "cnt": 1,
              "lastVisit": t
            };
          }
          await dynamo
            .put({
              TableName: pageHistoryTableName,
              Item: historyRow
            })
            .promise();

          if (!('restaurants' in body)) {
            body.restaurants = [];
          }
        } else {
          statusCode = 404;
          body = {'err': `Attraction with ID ${event.pathParameters.attractionId} doesn't exist!`};
        }
        break;
      default:
        throw new Error(`Unsupported route: "${event.routeKey}"`);
    }
  } catch (err) {
    statusCode = 400;
    console.log(err);
    body = {'err': err.message};
  } finally {
    body = JSON.stringify(body);
  }

  return {
    statusCode,
    body,
    headers
  };
};
