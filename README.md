{
  "employees": [
    {"id": 1, "name": "Alice", "salary": 20000},
    {"id": 2, "name": "Bob", "salary": 35000}
  ]
}
Below working functionality.

Upload JSON file to S3 bucket containing employee details (ID, name, salary).
S3 triggers a Lambda function whenever a file is uploaded.
Lambda function (written in Java) reads the JSON file and processes the data.
SNS topics notify:
Department A if salary = 20,000.
Department B if salary > 30,000.
SQS queue integrates with other services or logs.
