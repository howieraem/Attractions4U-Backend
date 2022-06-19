rm -f attractions4u-recommender.zip
zip -r attractions4u-recommender.zip .
aws lambda update-function-code --function-name attractions4u-recommender --zip-file fileb://./attractions4u-recommender.zip
