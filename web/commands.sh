aws s3 cp web/index.html s3://$(aws ssm get-parameter --name "/genai/cognito/BucketName" --query "Parameter.Value" --output text)/dea/index.html
