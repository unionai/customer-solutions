import base64
import json
import logging
import os
import signal
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import flyte
import flyte.remote as remote
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("subscriber")

# EVENT_SOURCE selects how a message becomes task inputs:
#   "json"  - the message body IS the inputs; splat it (you control the publisher)
#   "gcs"   - GCS object notification; routing is in attributes, data is object metadata
EVENT_SOURCE = os.environ.get("EVENT_SOURCE")

TASK = os.environ["FLYTE_TASK"]                    # e.g. gcs_ingest.on_object
RELEASE = os.environ.get("FLYTE_RELEASE", "prod")  # stable label, never a code version
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "20"))

# The API key encodes the endpoint, so nothing else is needed to connect.
# Reads FLYTE_API_KEY from the environment when called with no argument.
# This runs before the health server binds: if Union is unreachable at startup the
# process exits and the pod backs off, which is what you want.
flyte.init_from_api_key(
    project=os.environ["FLYTE_PROJECT"],
    domain=os.environ["FLYTE_DOMAIN"],
)
task = remote.Task.get(TASK, version=RELEASE)


def to_inputs(message: pubsub_v1.subscriber.message.Message) -> dict | None:
    """Turn a message into task inputs, or None if there is nothing to act on."""
    if EVENT_SOURCE == "gcs":
        # A GCS notification carries routing in attributes; `data` is the object
        # resource (kind, selfLink, md5Hash, ...) — metadata about the file, not
        # task inputs. Splatting it would pass a dozen unexpected kwargs.
        attrs = message.attributes
        bucket, obj = attrs.get("bucketId"), attrs.get("objectId")
        if not bucket or not obj:
            return None
        if attrs.get("eventType") != "OBJECT_FINALIZE":
            return None        # deletes and metadata updates arrive here too
        inputs = {"object_key": f"gs://{bucket}/{obj}"}
        if ts := attrs.get("eventTime"):
            inputs["event_time"] = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return inputs

    try:
        return json.loads(base64.b64decode(message.data).decode())
    except ValueError:
        return None


def handle(message: pubsub_v1.subscriber.message.Message) -> None:
    payload = to_inputs(message)
    if payload is None:
        log.info("nothing actionable in message %s; acking", message.message_id)
        message.ack()          # nothing actionable: ack so it stops redelivering
        return

    try:
        # Run name derived from messageId, so a duplicate delivery collides
        # instead of launching a second run.
        run = flyte.with_runcontext(name=f"ps-{message.message_id}").run(task, **payload)
        log.info("launched %s for message %s -> %s", run.name, message.message_id, run.url)
    except Exception as e:
        if "already exists" in str(e).lower():
            log.info("duplicate delivery of %s; run already exists", message.message_id)
        else:
            log.exception("failed to launch for message %s", message.message_id)
            message.nack()     # real failure: let Pub/Sub redeliver
            return

    message.ack()              # launched (or already running) — done


subscriber = pubsub_v1.SubscriberClient()
path = subscriber.subscription_path(os.environ["GCP_PROJECT"], os.environ["SUBSCRIPTION"])

# Flow control bounds how much is in flight. This is the main thing push cannot give you.
# In-flight work across the Deployment is replicas x max_messages.
future = subscriber.subscribe(
    path,
    callback=handle,
    flow_control=pubsub_v1.types.FlowControl(max_messages=MAX_MESSAGES),
)
log.info("subscribed to %s (source=%s, task=%s:%s, max_messages=%d)",
         path, EVENT_SOURCE, TASK, RELEASE, MAX_MESSAGES)


class _Health(BaseHTTPRequestHandler):
    """Liveness: 200 only while the StreamingPull is actually running.

    Catches the case where the stream resolved with an error on a background thread
    but the main thread has not yet unwound — a plain "process is up" probe would
    call that healthy. It cannot detect a stream that is connected but starved; use
    subscription backlog and oldest-unacked-message-age alerts for that.
    """

    def do_GET(self):
        ok = future.running()
        self.send_response(200 if ok else 503)
        self.end_headers()
        self.wfile.write(b"ok" if ok else b"stream not running")

    def log_message(self, *args):
        pass                   # keep kubelet probes out of the logs


threading.Thread(
    target=HTTPServer(("", 8080), _Health).serve_forever, daemon=True
).start()

# SIGTERM (rollout, eviction, node drain): stop pulling and let in-flight callbacks
# finish within terminationGracePeriodSeconds, rather than dying mid-launch and
# leaving those messages to wait out the ack deadline before redelivery.
signal.signal(signal.SIGTERM, lambda *_: future.cancel())

# Blocks until the stream is cancelled (returns) or fails unrecoverably (raises,
# process exits non-zero, kubelet restarts the pod).
future.result()
