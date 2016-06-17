# sqs-notify-s3-retrieve
Poll for SQS messages of S3 bucket object creation and retrieve objects locally.

## Usage
Launch the Docker container with the SQS queue you want to monitor. AWS Region, Access ID and Secret Key values
should all be specified as Docker environment parameters.

```
docker run -it -e AWS_DEFAULT_REGION=us-east-1 -e AWS_ACCESS_KEY_ID=your_access_key -e AWS_SECRET_ACCESS_KEY=your_secret_key gisjedi/sqs-notify-s3-retrieve my_queue_id -w
```

Get full usage options:

```
docker run -it gisjedi/sqs-notify-s3-retrieve
```