"""AWS Lambda that launches a Flyte 2 run per SQS message.

Deployed behind an SQS event source mapping: AWS polls the queue and invokes this
handler with a batch of messages. There is no long-running process to operate.

The handler launches runs and returns. It never waits for a run to finish — that
would burn Lambda duration and risk the visibility timeout, redelivering messages
that are already running.

Environment:
    FLYTE_API_KEY_SECRET   Secrets Manager secret id holding the Union API key
    FLYTE_PROJECT          Flyte project
    FLYTE_DOMAIN           Flyte domain
    FLYTE_TASK             e.g. s3_ingest.on_object
    FLYTE_RELEASE          release label the caller pins, e.g. prod
"""

import json
import logging
import os

import boto3
import flyte
import flyte.remote as remote

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("handler")

TASK = os.environ["FLYTE_TASK"]
RELEASE = os.environ.get("FLYTE_RELEASE", "prod")

# Module scope: this runs once per execution environment, during Lambda's init phase,
# and is reused by every warm invocation. `import flyte.remote` alone takes seconds, so
# doing it per-invocation would add that to every message.
_secret_id = os.environ["FLYTE_API_KEY_SECRET"]
_api_key = boto3.client("secretsmanager").get_secret_value(SecretId=_secret_id)["SecretString"]

flyte.init_from_api_key(
    api_key=_api_key,
    project=os.environ["FLYTE_PROJECT"],
    domain=os.environ["FLYTE_DOMAIN"],
)
_task = remote.Task.get(TASK, version=RELEASE)


def to_inputs(body: str) -> dict | None:
    """Map an S3 event notification to task inputs, or None if not actionable.

    S3 notifications delivered through SQS arrive as a JSON string in the message
    body, wrapping a list of records. The record describes the object; it is not the
    task's inputs, so it has to be mapped rather than passed through.
    """
    try:
        payload = json.loads(body)
    except ValueError:
        return None

    # A subscription confirmation or test event has no Records.
    records = payload.get("Records") or []
    for rec in records:
        if not rec.get("eventName", "").startswith("ObjectCreated"):
            continue
        s3 = rec.get("s3", {})
        bucket = s3.get("bucket", {}).get("name")
        key = s3.get("object", {}).get("key")
        if bucket and key:
            return {"object_key": f"s3://{bucket}/{key}"}
    return None


def handler(event, context):
    """Launch one run per message. Returns partial batch failures for redelivery."""
    failures = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        payload = to_inputs(record.get("body", ""))

        if payload is None:
            # Nothing actionable. Report success so SQS deletes it rather than
            # redelivering until the redrive policy sends it to the DLQ.
            log.info("no actionable content in %s", message_id)
            continue

        try:
            # Run name from the SQS messageId, so a redelivery collides with the
            # existing run instead of starting a second one.
            run = flyte.with_runcontext(name=f"sqs-{message_id}").run(_task, **payload)
            log.info("launched %s for %s", run.name, message_id)
        except Exception as e:
            if "already exists" in str(e).lower():
                log.info("duplicate delivery of %s; run exists", message_id)
                continue
            log.exception("launch failed for %s", message_id)
            failures.append({"itemIdentifier": message_id})

    # Requires ReportBatchItemFailures on the event source mapping. Without it, one
    # failure returns the whole batch to the queue and every message is retried.
    return {"batchItemFailures": failures}
