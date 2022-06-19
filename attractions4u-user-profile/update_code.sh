rm -f attractions4u-user-profile.zip
zip -r attractions4u-user-profile.zip .
aws lambda update-function-code --function-name attractions4u-user-profile --zip-file fileb://./attractions4u-user-profile.zip

