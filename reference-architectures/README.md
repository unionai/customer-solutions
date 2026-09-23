# Reference architectures

Opinionated, end-to-end designs for integrating Union with systems customers already run.

Each folder contains the architecture write-up, runnable code, and a walkthrough you can
follow in your own environment. They are starting points for a design conversation, not
drop-in products — every one lists the questions that would change the design.

| | |
|---|---|
| [pubsub-to-flyte](./pubsub-to-flyte/) | Launch Flyte 2 tasks from Google Cloud Pub/Sub. A pull subscriber runs as a Union App; the worked example reacts to files landing in GCS. |
| [sqs-lambda-to-flyte](./sqs-lambda-to-flyte/) | Launch Flyte 2 tasks from AWS SQS via a Lambda event source mapping. The worked example reacts to objects landing in S3. |
