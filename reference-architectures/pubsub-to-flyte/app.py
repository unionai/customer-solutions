"""Pub/Sub subscriber as a Union App, structured as a FastAPI app.

Deploy:
    flyte serve app.py app_env

Requirements, one time:

  1. The task this launches, deployed to the same project and domain:

       flyte deploy --version r1 gcs_ingest.py env

  2. A Union API key stored as the secret `flyte-api-key`, which the app reads from
     FLYTE_API_KEY.

  3. Google credentials for Pub/Sub, reaching the app through Application Default
     Credentials.
"""

import base64
import json
import logging
import os
from datetime import datetime

import flyte
import flyte.remote as remote
from fastapi import FastAPI
from flyte.app.extras import FastAPIAppEnvironment
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("subscriber")

EVENT_SOURCE = os.environ.get("EVENT_SOURCE", "gcs")
TASK = os.environ["FLYTE_TASK"]
RELEASE = os.environ.get("FLYTE_RELEASE", "prod")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "20"))

_future = None


def to_inputs(message) -> dict | None:
    """Map a message to task inputs, or None if there is nothing to act on."""
    if EVENT_SOURCE == "gcs":
        # GCS notifications carry routing in attributes; `data` is the object
        # resource (kind, selfLink, md5Hash, ...), which is metadata about the
        # file rather than task inputs.
        attrs = message.attributes
        bucket, obj = attrs.get("bucketId"), attrs.get("objectId")
        if not bucket or not obj:
            return None
        if attrs.get("eventType") != "OBJECT_FINALIZE":
            return None
        inputs = {"object_key": f"gs://{bucket}/{obj}"}
        if ts := attrs.get("eventTime"):
            inputs["event_time"] = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return inputs

    try:
        return json.loads(base64.b64decode(message.data).decode())
    except ValueError:
        return None


def handle(message) -> None:
    payload = to_inputs(message)
    if payload is None:
        log.info("nothing actionable in %s; acking", message.message_id)
        message.ack()
        return

    try:
        # Run name from messageId, so a duplicate delivery collides with the first
        # run rather than starting a second. Flyte enforces uniqueness, so this
        # holds across replicas.
        task = remote.Task.get(TASK, version=RELEASE)
        run = flyte.with_runcontext(name=f"ps-{message.message_id}").run(task, **payload)
        log.info("launched %s for %s -> %s", run.name, message.message_id, run.url)
    except Exception as e:
        if "already exists" in str(e).lower():
            log.info("duplicate delivery of %s; run exists", message.message_id)
        else:
            log.exception("launch failed for %s", message.message_id)
            message.nack()          # real failure: let Pub/Sub redeliver
            return

    message.ack()                   # ack on launch, never on completion


app = FastAPI()


@app.get("/health")
def health():
    """200 only while the StreamingPull is running, so a stalled stream is visible."""
    running = _future is not None and _future.running()
    return ({"status": "ok"}, 200) if running else ({"status": "stream not running"}, 503)


image = (
    flyte.Image.from_debian_base(python_version=(3, 12))
    .with_pip_packages("google-cloud-pubsub", "fastapi", "uvicorn")
)

app_env = FastAPIAppEnvironment(
    name="pubsub-subscriber",
    app=app,
    image=image,
    secrets=[flyte.Secret("flyte-api-key", as_env_var="FLYTE_API_KEY")],
    # Always-on: apps default to scale-to-zero and autoscale on request volume,
    # which a subscriber never generates.
    scaling=flyte.app.Scaling(replicas=(1, 1)),
    resources=flyte.Resources(cpu="1", memory="1Gi"),
    env_vars={
        "EVENT_SOURCE": "gcs",
        "FLYTE_TASK": "gcs_ingest.on_object",
        "FLYTE_RELEASE": "r1",
        "GCP_PROJECT": "<GCP_PROJECT>",
        "SUBSCRIPTION": "<SUBSCRIPTION>",
    },
)


@app_env.on_startup
async def start_subscriber():
    """Authenticate, then start the pull loop. Runs before the server accepts traffic."""
    global _future

    # The platform injects project and domain into the pod, so neither needs to be
    # configured. The API key carries the endpoint and credentials, read from
    # FLYTE_API_KEY (mounted from the `flyte-api-key` secret).
    flyte.init_from_api_key(
        project=flyte.current_project(),
        domain=flyte.current_domain(),
    )
    log.info("authenticated to %s/%s", flyte.current_project(), flyte.current_domain())

    subscriber = pubsub_v1.SubscriberClient()
    path = subscriber.subscription_path(os.environ["GCP_PROJECT"], os.environ["SUBSCRIPTION"])
    _future = subscriber.subscribe(
        path,
        callback=handle,
        flow_control=pubsub_v1.types.FlowControl(max_messages=MAX_MESSAGES),
    )
    log.info("subscribed to %s (source=%s, task=%s:%s)", path, EVENT_SOURCE, TASK, RELEASE)


if __name__ == "__main__":
    flyte.init_from_config()
    print(flyte.deploy(app_env)[0])
