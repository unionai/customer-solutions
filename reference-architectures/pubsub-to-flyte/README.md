# Launching Flyte 2 tasks from Google Cloud Pub/Sub

**Reference architecture**

## Contents

| File | |
|---|---|
| `subscriber.py` | the pull subscriber — set `EVENT_SOURCE=gcs` for object notifications, `json` if you publish your own payloads |
| `gcs_ingest.py` | example Flyte task accepting an object key |
| `Dockerfile`, `requirements.txt` | container for the subscriber |
| `k8s/deployment.yaml` | Deployment manifest, with placeholders to fill in |

## Scope

How to run a Flyte 2 task in response to a Pub/Sub message: the recommended pattern,
what has to be configured, the failure modes that matter in production, and a working
example you can stand up yourself.

The worked example reacts to files landing in a GCS bucket, since object notifications
are the most common source. The pattern is the same for any publisher.

---

## 1. Recommended pattern

A **pull subscriber that you own** — an ordinary long-running Python process using the
Pub/Sub client library and the Flyte SDK. It reads the subscription and launches one
Flyte run per message.

```
   GCS bucket                    your cluster                    Union
   ──────────                    ────────────                    ─────
   object lands
   trainingdata/*.nc
        │
        │ notification
        ▼
   Pub/Sub topic
        │
        │ StreamingPull  ─────▶  subscriber pod
        │   (outbound)           ├─ map event → task inputs
        │                        ├─ launch run  ──────────────▶  Flyte task run
        │                        └─ ack
        ▼
   dead-letter topic
   (after N failures)
```

The subscriber opens **outbound** connections in both directions. It exposes no
endpoint, receives no inbound traffic, and requires no ingress.

### Why pull rather than push

A push subscription delivering to an HTTPS endpoint is the obvious alternative. We
recommend against it here for three reasons:

**Authentication.** Pub/Sub push authenticates with a Google-issued OIDC token and
cannot send a Union credential. Any push receiver must therefore verify Google tokens
itself, on a publicly reachable endpoint. Pull removes the problem rather than solving
it: the subscriber authenticates outbound to both Google and Union, and nothing is
exposed.

**Flow control.** The pull client bounds in-flight work (`max_messages`). Push
subscriptions deliver as fast as they can and the receiver absorbs it. When a backlog
drains, that difference decides whether you launch a controlled number of runs or a
flood of them.

**Batching.** Accumulating several messages into one run is a few lines in a pull
callback. It is awkward in a request handler, and this need arrives sooner than teams
expect.

The cost of pull is that it is a long-running process rather than something that scales
to zero. If a hard scale-to-zero requirement exists, push to Cloud Run is the fallback —
Google verifies the token via IAM and nothing is public — accepting the loss of flow
control.

---

## 2. Components and ownership

| Component | Owner | Notes |
|---|---|---|
| Pub/Sub topic, subscription, dead-letter topic | Customer | ordinary GCP resources |
| Bucket notification config | Customer | if the source is GCS |
| Subscriber process and where it runs | Customer | GKE Deployment or Cloud Run |
| Mapping message → task inputs | Customer | the only genuinely bespoke code |
| Flyte task and its releases | Customer | deployed to Union |
| Union API key | Customer | stored in their secret manager |

The integration is deliberately customer-owned. It is ordinary Python against two
documented client libraries, and it keeps event plumbing inside their own
infrastructure.

---

## 3. Configuration requirements

### 3.1 Pub/Sub

| Setting | Recommendation |
|---|---|
| Subscription type | pull |
| Ack deadline | 60s — the client extends it while the callback runs |
| Message retention | 7 days |
| Dead-letter topic | required |
| Max delivery attempts | 5 |
| Flow control | `max_messages` sized against tolerable concurrent runs |

If the source is GCS, the notification config should filter server-side by object prefix
and event type, so unrelated bucket activity never reaches the subscriber.

### 3.2 IAM

Four grants, and three of them are commonly missed:

| Principal | Role | On | Why |
|---|---|---|---|
| GCS service agent | `pubsub.publisher` | the topic | without it the notification config exists but nothing is delivered |
| Pub/Sub service agent | `pubsub.publisher` | dead-letter topic | without it messages retry forever instead of dead-lettering |
| Pub/Sub service agent | `pubsub.subscriber` | the subscription | same |
| Subscriber's Google SA | `pubsub.subscriber` | the subscription | the only one that is obvious |

Scope grants to the specific topic or subscription rather than the project.

### 3.3 Workload Identity

Two halves, and both are required:

1. The Kubernetes SA is annotated with `iam.gke.io/gcp-service-account`
2. The Google SA has an `iam.workloadIdentityUser` binding for
   `PROJECT.svc.id.goog[NAMESPACE/KSA_NAME]`

**The namespace is part of the identity.** A binding for `[default/subscriber]` grants
nothing to a pod running in another namespace. This misconfiguration is easy to miss
because both halves look correct in isolation and nothing fails until the pod
authenticates.

Note that the **image pull** does not use this identity. The kubelet pulls using the
node pool's service account before the container exists, so the workload's service
account never needs registry access.

### 3.4 Runtime

The subscriber is a long-running process, so it is a **Deployment** — not a Job or
CronJob, which would tear down the subscription connection. It needs no Service or
Ingress.

On Cloud Run it requires `--min-instances=1` and `--no-cpu-throttling`; without the
latter the CPU is throttled between requests and a background subscriber stalls.

### 3.5 Which task version runs

The subscriber should name a **release label**, never a code version, and should not use
"latest" resolution — that would make every deploy an immediate production change with
no way to pin or roll back.

Deploy each release twice: once under an immutable tag that is a permanent record, and
once under a label the subscriber pins.

```bash
flyte deploy --version 2026-09-04-a3f9c21  pipeline.py env   # immutable record
flyte deploy --version prod                pipeline.py env   # what production runs
```

Releases and rollbacks then happen entirely on the Flyte side; the subscriber never
changes. Deploy with meaningful versions — a git SHA or release tag — since auto-generated
versions are content hashes and unpleasant to promote by hand.

---

## 4. Reliability semantics

These decide whether the integration behaves under load and failure. They are properties
of Pub/Sub, not of Flyte, and apply to any design.

**Delivery is at-least-once.** Duplicates will happen. Derive the Flyte run name from the
Pub/Sub `messageId`: run names are unique per project and domain, so a redelivery
collides with the existing run rather than starting a second one. Treat "already exists"
as success. This holds across replicas, because uniqueness is enforced by the platform
rather than by subscriber state.

**Ack when the run is created, never when it finishes.** Blocking on completion exhausts
the ack deadline and triggers redelivery, which launches duplicates.

**Dead-letter the unprocessable.** A message that can never be parsed will otherwise
retry indefinitely. Ack permanently-bad messages rather than nacking them, and attach a
dead-letter topic for the rest.

**Ordering keys order delivery, not completion.** Two runs launched in order can finish
out of order. If a pipeline must not run concurrently for a given key, enforce that in
Flyte rather than at the transport.

**Retries have two owners.** Flyte task retries and Pub/Sub redelivery will otherwise
compete. The usual split: ack on successful launch, let Flyte own execution retries, and
reserve the dead-letter queue for messages that never launched.

---

## 5. Mapping messages to task inputs

The only bespoke code. It is tempting to decode the message body and splat it into the
task, which works when you control the publisher and shaped the payload to match the task
signature. It does not work for cloud-generated events.

GCS object notifications put routing data in `attributes` and the full object *resource*
in `data`:

```
attributes:  bucketId, objectId, eventType, eventTime
data:        base64 JSON — {kind, id, selfLink, name, bucket, generation,
                            size, md5Hash, contentType, ...}
```

That payload is metadata about the file, not task inputs. Read `attributes` and construct
inputs explicitly:

```python
inputs = {"object_key": f"gs://{attrs['bucketId']}/{attrs['objectId']}"}
```

Filter on `eventType` as well — a bucket configured for multiple event types delivers
deletes and metadata updates through the same subscription.

---

## 6. Observability

**Log every decision.** A subscriber that launches runs silently is indistinguishable
from one that is wedged. Log on launch (with the run name and URL), on duplicate
delivery, on skip, and on failure.

**Alert on the subscription, not the pod.** The signals that matter:

| Signal | Catches |
|---|---|
| `num_undelivered_messages` (backlog) | subscriber down or too slow |
| oldest unacked message age | connected but wedged |
| dead-letter topic depth | messages that never launched a run |
| container restarts | crash loop, which otherwise looks like backlog growth |

**Liveness needs care.** A StreamingPull subscriber can wedge while the process stays
alive, and with no HTTP server there is nothing to probe by default. Either expose a
small health endpoint reporting whether the stream is still running, or rely on the
backlog alerts above.

---

## 7. Try it yourself

Roughly 30-45 minutes. Substitute your own project, bucket, and cluster.

Prerequisites: a Union instance you can log into, with a project and domain created, and
the `flyte` CLI configured (`flyte create config --endpoint dns:///<your-endpoint>`).

**1. Create the topic and let GCS publish to it**

```bash
gcloud pubsub topics create trainingdata-uploads

GCS_SA=$(gcloud storage service-agent --project="$PROJECT")
gcloud pubsub topics add-iam-policy-binding trainingdata-uploads \
  --member="serviceAccount:${GCS_SA}" --role=roles/pubsub.publisher
```

**2. Notify on new objects under a prefix**

```bash
gcloud storage buckets notifications create gs://YOUR_BUCKET \
  --topic=projects/$PROJECT/topics/trainingdata-uploads \
  --event-types=OBJECT_FINALIZE \
  --object-prefix=trainingdata/ \
  --payload-format=json
```

**3. Create the subscription and its dead-letter path**

```bash
gcloud pubsub subscriptions create trainingdata-uploads-sub \
  --topic=trainingdata-uploads --ack-deadline=60 \
  --message-retention-duration=7d \
  --dead-letter-topic=deadletter --max-delivery-attempts=5

PN=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
PSA="service-${PN}@gcp-sa-pubsub.iam.gserviceaccount.com"
gcloud pubsub topics add-iam-policy-binding deadletter \
  --member="serviceAccount:${PSA}" --role=roles/pubsub.publisher
gcloud pubsub subscriptions add-iam-policy-binding trainingdata-uploads-sub \
  --member="serviceAccount:${PSA}" --role=roles/pubsub.subscriber
```

**4. Verify the plumbing before writing any code**

```bash
gcloud storage cp sample.txt gs://YOUR_BUCKET/trainingdata/sample.txt
gcloud pubsub subscriptions pull trainingdata-uploads-sub --limit=1 --format=json
```

A message with `attributes.objectId` set means topic, IAM, notification and subscription
are all correct. Everything after this point is application code. Leave it unacked and
the subscriber will consume it on first start.

**5. Deploy a task that accepts the object**

```python
@env.task
async def on_object(object_key: str = "", event_time: datetime = EPOCH) -> str:
    return f"processing {object_key}"
```

```bash
flyte deploy --version r1   gcs_ingest.py env
flyte deploy --version prod gcs_ingest.py env
```

**6. Mint the Union API key**

The subscriber authenticates to Union with an API key. It encodes the endpoint, client
id and secret in a single string, so it is the only Union credential the pod needs.

The command ships in the `flyteplugins-union` package rather than the base CLI:

```bash
uv add --dev flyteplugins-union      # or: pip install flyteplugins-union

# from a machine already logged in to your Union instance
uv run flyte create api-key --name pubsub-subscriber-key
```

The output is shown **once** — copy it immediately. Store it in Secret Manager and mount
it as `FLYTE_API_KEY`:

```bash
echo -n "<the key>" | gcloud secrets create flyte-api-key --data-file=-
gcloud secrets add-iam-policy-binding flyte-api-key \
  --member="serviceAccount:flyte-subscriber@$PROJECT.iam.gserviceaccount.com" \
  --role=roles/secretmanager.secretAccessor
```

The key inherits the permissions of the user who minted it, so mint it from a dedicated
service identity scoped to the target project and domain rather than a personal account.
Rotate on a schedule — 90 days is a reasonable default — by minting a new key and
updating the secret.

**7. Grant the subscriber identity and wire Workload Identity**

```bash
gcloud iam service-accounts create flyte-subscriber
gcloud pubsub subscriptions add-iam-policy-binding trainingdata-uploads-sub \
  --member="serviceAccount:flyte-subscriber@$PROJECT.iam.gserviceaccount.com" \
  --role=roles/pubsub.subscriber

kubectl create serviceaccount flyte-subscriber -n YOUR_NS
kubectl annotate serviceaccount flyte-subscriber -n YOUR_NS \
  iam.gke.io/gcp-service-account=flyte-subscriber@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts add-iam-policy-binding \
  flyte-subscriber@$PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:$PROJECT.svc.id.goog[YOUR_NS/flyte-subscriber]"
```

The namespace in that last member string must match the Deployment's namespace.

**8. Build, push, deploy**

```bash
gcloud builds submit --tag REGION-docker.pkg.dev/$PROJECT/REPO/flyte-subscriber:v1 .
kubectl apply -f deployment.yaml
kubectl logs -n YOUR_NS -l app=flyte-subscriber -f
```

**9. Trigger it**

```bash
gcloud storage cp file.txt gs://YOUR_BUCKET/trainingdata/file-$(date +%s).txt
```

A run should appear in the Flyte console within seconds.

---

## 8. Open questions

To size and finalise this for your environment:

1. Where will the subscriber run, and is scale-to-zero a hard requirement?
2. What is the message volume and burst profile, and is it one message per run? At
   thousands per minute, batching changes the design.
3. Is processing the same message twice harmful, or merely wasteful?
4. How do you decide which code version production runs, and do you need to roll back
   without redeploying?
5. Does per-key ordering matter?
6. What language is the subscriber written in? Python means the SDK and none of the
   raw-API work below.

---

## Appendix: callers that are not Python

Flyte 2's control plane is served over Connect RPC, which accepts plain `POST` + JSON
over HTTP/1.1 — no protobuf toolchain or code generation required.

```
POST /cloudidl.workflow.RunService/CreateRun
Authorization: Bearer <token>
```

The awkward part is not the transport but the inputs: task inputs are typed protobuf
literals, so an integer is `{"scalar": {"primitive": {"integer": "42"}}}` and files are
more involved. Two ways around it — give the task a single string input carrying the raw
message and parse inside the task, or put a thin Python service in front. The first is
usually right; keeping the subscriber in Python avoids the question entirely.
