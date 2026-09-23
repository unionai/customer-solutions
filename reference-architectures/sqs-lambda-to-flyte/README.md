# Launching Flyte 2 tasks from AWS SQS

**Reference architecture — draft for discussion**

## Contents

| File | |
|---|---|
| `lambda_handler.py` | the Lambda, launching one run per message |
| `s3_ingest.py` | example task, accepting an object key |
| `requirements.txt` | packaging dependencies |

## Scope

Running a Flyte 2 task in response to an SQS message: the architecture, what has to be
configured, the failure modes that matter, and a walkthrough you can follow.

The example reacts to objects arriving in S3, since bucket notifications are the most
common source. The pattern is the same for any publisher.

---

## 1. Choosing an approach

Two designs, and the choice is where the event comes from.

**Inside Flyte — use an artifact trigger.** A task publishes a new version of a named
artifact, and any task with an `OnArtifact` trigger runs automatically with the artifact
as an input. No queue, no Lambda, no credentials to store.

```python
on_new_data = flyte.Trigger(
    name="on_new_dataset",
    automation=flyte.OnArtifact(name="incoming_dataset"),
    inputs={"dataset": flyte.TriggeredArtifact},
)
```

The run is linked to the artifact version that fired it, so there is no state to keep
outside the platform, and `TriggeredArtifact` delivers a typed artifact straight to the
task input — nothing decodes a payload or maps fields.

**Outside Flyte — observe the event.** If data arrives from a system you do not control,
nothing publishes an artifact. The event has to be observed, which is what this document
covers.

### The cost of a queue

A Lambda has to return before the run finishes. Waiting would burn duration and risk the
visibility timeout, which would redeliver messages whose runs are already going. So a
successful return means *launched*, not *succeeded*, and the message is then deleted.

If that run later fails, SQS has no idea. Flyte retries within a run, but a run that ends
terminally failed leaves no message to redeliver and nothing to send to the DLQ. Closing
that gap means owning a reconciliation of your own: which messages produced runs, how
each ended, which failures to re-fire, and how a deliberate re-fire avoids colliding with
the `messageId`-derived run name that exists to prevent duplicates.

An artifact trigger has none of this. Prefer it where the producer is a Flyte task.

---

## 2. Architecture

```
   S3 bucket                    AWS                          Flyte
   ─────────                    ───                          ─────
   object lands
   trainingdata/*.csv
        │
        │ notification
        ▼
   SQS queue
        │
        │ event source mapping (AWS polls)
        ▼
   Lambda ──────────────────────────────────────────────▶   task run
        │  ├─ map event → task inputs
        │  ├─ launch run
        │  └─ return
        ▼
   dead-letter queue
   (after maxReceiveCount)
```

Nothing long-running. AWS polls the queue and invokes the function; the function launches
runs and returns.

### Why Lambda rather than a subscriber

On AWS the event source mapping already does the polling, so a dedicated subscriber
process would duplicate it. Lambda also gives concurrency limits, batching, partial batch
failures, and a DLQ as configuration rather than code.

The trade is cold starts. `import flyte.remote` takes roughly 4 seconds, and a clean
`flyte` install is about 83 MB — see section 3.4.

---

## 3. Configuration

### 3.1 SQS

| Setting | Recommendation |
|---|---|
| Queue type | standard, unless you need per-key ordering |
| Visibility timeout | at least 6× the Lambda timeout (AWS guidance) |
| Message retention | 7 days |
| Redrive policy | DLQ with `maxReceiveCount` of about 5 |

For an S3 source, filter by prefix and event type in the bucket notification so unrelated
activity never reaches the queue.

### 3.2 Event source mapping

| Setting | Why |
|---|---|
| `--batch-size` | messages per invocation; each becomes one run |
| `--maximum-batching-window-in-seconds` | trades latency for fewer invocations |
| `--function-response-types ReportBatchItemFailures` | **required** — see below |
| `--scaling-config MaximumConcurrency=N` | bounds concurrent runs, the equivalent of flow control |

Without `ReportBatchItemFailures`, one failed message fails the whole batch and every
message in it is redelivered — including the ones whose runs already launched.

### 3.3 IAM

| Principal | Needs |
|---|---|
| Lambda execution role | `AWSLambdaSQSQueueExecutionRole` — grants `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` plus logs |
| Lambda execution role | `secretsmanager:GetSecretValue` on the API key secret |
| S3 | a queue policy allowing `s3.amazonaws.com` to `sqs:SendMessage`, conditioned on the source bucket ARN |

### 3.4 Packaging

Measured for `flyte` alone, built for `x86_64`:

| | |
|---|---|
| Unzipped | 101 MB (limit 250 MB) |
| Zipped | 29 MB (limit 50 MB for direct upload) |

Do not package `boto3` — the Lambda runtime provides it. Including it adds roughly 27 MB
unzipped and pushes the zip to 45 MB, close enough to the 50 MB direct-upload limit to
cause trouble later.

`flyte` ships native extensions, so the package must be built for the Lambda
architecture rather than your laptop. Building on an arm64 Mac without pinning the
platform produces Darwin binaries that fail at import.

`import flyte.remote` takes about **4 seconds**. Do the import, the secret fetch and
`flyte.init_from_api_key()` at module scope so they run once during the init phase and
are reused by warm invocations. Lambda's init phase has a 10-second ceiling, so watch it
if you add dependencies, and use provisioned concurrency if first-message latency
matters.

### 3.5 Handler environment

| Variable | What it is |
|---|---|
| `FLYTE_TASK` | Fully qualified name of the task to launch: the `TaskEnvironment` name, a dot, then the function name. A task `on_object` in `TaskEnvironment(name="s3_ingest")` is `s3_ingest.on_object`. Find it with `flyte get task`. |
| `FLYTE_RELEASE` | The release label to pin, e.g. `prod`. A label, not a code version — see section 3.7. |
| `FLYTE_PROJECT` | Flyte project the runs are created in. |
| `FLYTE_DOMAIN` | Flyte domain, e.g. `development`. |
| `FLYTE_API_KEY_SECRET` | Secrets Manager secret **id**, not the key itself. The handler reads it once at init. |

The project and domain are where the *run* is created, which must be where the task was
deployed. Deploying to one and launching from another gives a task-not-found error.

### 3.6 Credentials

Store a Union API key in Secrets Manager and read it at module scope. Mint it from a
service identity scoped to the target project and domain rather than a personal account,
since the key inherits the permissions of whoever created it. Rotate on a schedule.

### 3.7 Which task version runs

The handler should name a release label, never a code version, and should not resolve
"latest" — that makes every deploy an immediate production change with no way to pin or
roll back.

```bash
flyte deploy --version 2026-09-15-a3f9c21  s3_ingest.py env   # immutable record
flyte deploy --version prod                s3_ingest.py env   # what production runs
```

Releases and rollbacks then happen on the Flyte side; the Lambda never changes.

---

## 4. Reliability

These are the price of putting a queue between the event and the run.

**Delivery is at-least-once.** Standard queues can deliver a message more than once.
Derive the run name from the SQS `messageId`: run names are unique per project and
domain, so a redelivery collides with the existing run instead of starting a second one.
Treat "already exists" as success.

**Return before the run finishes.** Blocking until completion burns Lambda duration and
can exceed the visibility timeout, which redelivers messages whose runs are already
running.

**Report partial batch failures.** Return `{"batchItemFailures": [...]}` so only the
messages that failed to launch are redelivered.

**Ack what cannot be processed.** A message that never parses will otherwise be retried
until it reaches the DLQ. Returning success for it is usually right; the DLQ should hold
genuine failures.

**FIFO queues order delivery, not completion.** Two runs launched in order can finish out
of order. If a pipeline must not run concurrently for a given key, enforce that in Flyte.

---

## 5. Mapping messages to task inputs

The only bespoke code on this path, and the part an artifact trigger does not need.

S3 notifications delivered through SQS arrive as a JSON string in the message body,
wrapping a list of records:

```json
{"Records": [{"eventName": "ObjectCreated:Put",
              "s3": {"bucket": {"name": "my-bucket"},
                     "object": {"key": "trainingdata/file.csv"}}}]}
```

That record describes the object; it is not the task's inputs. Build them explicitly:

```python
inputs = {"object_key": f"s3://{bucket}/{key}"}
```

Filter on `eventName` as well, and expect a `s3:TestEvent` message when the notification
is first configured — it has no `Records` and must not be treated as an error.

This mapping is yours to maintain. It has no type checking against the task signature and
breaks silently when either side changes.

---

## 6. Observability

**Log every decision** — on launch with the run name, on duplicate delivery, on skip, and
on failure.

**Alert on the queue, not the function.**

| Signal | Catches |
|---|---|
| `ApproximateAgeOfOldestMessage` | consumer stalled or too slow |
| `ApproximateNumberOfMessagesVisible` | backlog growth |
| DLQ depth | messages that never launched a run |
| Lambda `Errors` and `Throttles` | launch failures, or concurrency limits being hit |

---

## 7. Try it yourself

Substitute your own account, bucket, and queue.

**1. Create the queue and its dead-letter queue**

`create-queue` returns the URL, so capture it rather than copying it by hand. The ARN
comes from the queue attributes.

```bash
DLQ_URL=$(aws sqs create-queue --queue-name trainingdata-dlq \
  --query QueueUrl --output text)

DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)
```

`RedrivePolicy` is a JSON string nested inside a JSON map, so build it with `jq` instead
of escaping quotes by hand:

```bash
ATTRS=$(jq -nc --arg arn "$DLQ_ARN" '{
  VisibilityTimeout: "180",
  MessageRetentionPeriod: "604800",
  RedrivePolicy: ({deadLetterTargetArn: $arn, maxReceiveCount: "5"} | tostring)
}')

QUEUE_URL=$(aws sqs create-queue --queue-name trainingdata-uploads \
  --attributes "$ATTRS" --query QueueUrl --output text)

QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)
```

If you need to resume/restart the process on a new shell, recover both env vars without recreating anything:

```bash
QUEUE_URL=$(aws sqs get-queue-url --queue-name trainingdata-uploads \
  --query QueueUrl --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)
```

**2. Let S3 publish to the queue**

The queue policy has to exist **before** the notification is configured. S3 tests that it
can send to the destination when you save the configuration.

The queue and the bucket must also be in the same region; S3 does not deliver
notifications across regions.

```bash
BUCKET=your-bucket

POLICY=$(jq -nc --arg q "$QUEUE_ARN" --arg b "arn:aws:s3:::$BUCKET" '{
  Version: "2012-10-17",
  Statement: [{
    Effect: "Allow",
    Principal: {Service: "s3.amazonaws.com"},
    Action: "sqs:SendMessage",
    Resource: $q,
    Condition: {ArnLike: {"aws:SourceArn": $b}}
  }]}')

aws sqs set-queue-attributes --queue-url "$QUEUE_URL" \
  --attributes "$(jq -nc --arg p "$POLICY" '{Policy: $p}')"
```

Then configure the notification. Note `--bucket` takes the bucket **name**, while the
destination and the policy condition take ARNs:

```bash
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "'"$QUEUE_ARN"'",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "trainingdata/"}]}}
    }]}'
```

**3. Verify the plumbing before writing any code**

```bash
aws s3 cp sample.txt s3://YOUR_BUCKET/trainingdata/sample.txt
aws sqs receive-message --queue-url "$QUEUE_URL" --max-number-of-messages 1
```

A message whose body contains `ObjectCreated` means bucket, policy, notification and
queue are correct. Everything after this is application code.

**4. Deploy the task**

```bash
flyte deploy --version r1 s3_ingest.py env
```

**5. Store the API key**

`flyte create api-key` ships in the `flyteplugins-union` package rather than the base CLI.

```bash
uv add --dev flyteplugins-union
uv run flyte create api-key --name sqs-lambda-key      # shown once

aws secretsmanager create-secret --name flyte/api-key --secret-string '<the key>'
```

**6. Package and deploy the Lambda**

Build the dependencies for the Lambda architecture, not your laptop. `--only-binary`
makes a mismatch fail loudly instead of silently compiling for the wrong platform.

```bash
rm -rf build && mkdir build

uv pip install --target build \
  --python-platform x86_64-manylinux2014 --python-version 3.12 \
  --only-binary=:all: flyte

cp lambda_handler.py build/
(cd build && zip -qr ../function.zip .)
```

Use `aarch64-manylinux2014` instead if the function runs on Graviton.

Create the execution role:

```bash
ROLE_ARN=$(aws iam create-role --role-name flyte-sqs-launcher \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{
    "Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},
    "Action":"sts:AssumeRole"}]}' \
  --query Role.Arn --output text)

aws iam attach-role-policy --role-name flyte-sqs-launcher \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole

aws iam put-role-policy --role-name flyte-sqs-launcher \
  --policy-name read-flyte-api-key \
  --policy-document "$(jq -nc --arg s "$SECRET_ARN" '{
    Version:"2012-10-17",
    Statement:[{Effect:"Allow",Action:"secretsmanager:GetSecretValue",Resource:$s}]}')"
```

Then create the function. The timeout only has to cover launching runs, not running
them:

```bash
aws lambda create-function --function-name flyte-sqs-launcher \
  --runtime python3.12 --architectures x86_64 \
  --handler lambda_handler.handler \
  --role "$ROLE_ARN" \
  --zip-file fileb://function.zip \
  --timeout 30 --memory-size 1024 \
  --environment "Variables={\
FLYTE_API_KEY_SECRET=flyte/api-key,\
FLYTE_PROJECT=your-project,\
FLYTE_DOMAIN=development,\
FLYTE_TASK=s3_ingest.on_object,\
FLYTE_RELEASE=prod}"
```

IAM role creation is eventually consistent; if it reports that the role cannot be
assumed, wait a few seconds and retry.

Check that init succeeds before wiring the queue, since a failure there shows up as
every message failing:

```bash
aws lambda invoke --function-name flyte-sqs-launcher \
  --payload '{"Records":[]}' --cli-binary-format raw-in-base64-out /dev/stdout
```

An empty `batchItemFailures` means the import, secret fetch and Flyte auth all worked.

To update code later:

```bash
aws lambda update-function-code --function-name flyte-sqs-launcher \
  --zip-file fileb://function.zip
```

**7. Wire the queue to the function**

```bash
aws lambda create-event-source-mapping \
  --function-name flyte-sqs-launcher \
  --event-source-arn "$QUEUE_ARN" \
  --batch-size 10 \
  --maximum-batching-window-in-seconds 5 \
  --function-response-types ReportBatchItemFailures \
  --scaling-config MaximumConcurrency=5
```

**8. Trigger it**

```bash
aws s3 cp file.txt s3://YOUR_BUCKET/trainingdata/file-$(date +%s).txt
```

A run appears in the Flyte console within seconds.

---

## 8. Open questions

1. What is the message volume and burst profile, and is it one message per run? At high
   rates, batching several messages into one run changes the design.
2. Is processing the same message twice harmful, or only wasteful?
3. How do you decide which code version production runs, and do you need to roll back
   without redeploying?
4. Does per-key ordering matter? That decides standard versus FIFO.
5. Should runs be attributed to the originating user or tenant? The Lambda calls Flyte
   with its own credentials, so that identity has to travel in the message and be
   enforced in the pipeline.
