rm -f attractions4u-cache.zip
zip -r attractions4u-cache.zip .
aws lambda update-function-code --function-name attractions4u-cache --zip-file fileb://./attractions4u-cache.zip

