rm -f attractions4u-attraction-py.zip
zip -r attractions4u-attraction-py.zip .
aws lambda update-function-code --function-name attractions4u-attraction-py --zip-file fileb://./attractions4u-attraction-py.zip
