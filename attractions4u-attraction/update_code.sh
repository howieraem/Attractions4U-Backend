rm -f attractions4u-attraction.zip
zip -r attractions4u-attraction.zip .
aws lambda update-function-code --function-name attractions4u-attraction --zip-file fileb://./attractions4u-attraction.zip

