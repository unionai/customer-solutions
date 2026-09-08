# Launching Flyte 2 tasks from Google Cloud Pub/Sub

**Reference architecture — draft for discussion**

## Contents

| File | |
|---|---|
| `app.py` | the subscriber, deployed as a Union App |
| `gcs_ingest.py` | example task, accepting an object key |

## Scope

Running a Flyte 2 task in response to a Pub/Sub message: the architecture, what has to
be configured, the failure modes that matter in production, and a walkthrough you can
follow in your own environment.

The example reacts to files arriving in a GCS bucket, since object notifications are the
most common source. The pattern is the same for any publisher.

---

## 1. Architecture

A **pull subscriber running as a Union App**. It reads the subscription and launches one
Flyte run per message.

```
   GCS bucket                  Union App                      Flyte
   ──────────                  ─────────                      ─────
   object lands
   trainingdata/*.nc
        │
        │ notification
        ▼
   Pub/Sub topic
        │
        │ StreamingPull  ──▶   subscriber
        │                      ├─ map event → task inputs
        │                      ├─ launch run  ──────────────▶  task run
        │                      └─ ack
        ▼
   dead-letter topic
   (after N failures)
```

The subscriber opens outbound connections in both directions. It exposes no endpoint and
receives no inbound traffic; the HTTP port exists only for health checks.

### Why an App

Union App Serving runs any long-lived process, so the subscriber deploys with
`flyte deploy` and Union operates it. Compared with running the same process yourself:

- **No cluster resources to maintain.** No Deployment manifest, ServiceAccount, image
  build pipeline, or `kubectl` rollout.
- **Project and domain come from the platform.** `flyte.current_project()` and
  `flyte.current_domain()` read what Union injects into the pod, so the app knows where
  it is without configuration.
- **The image is built for you.** `flyte.Image` declares dependencies; the remote builder
  produces and stores the image.
- **Standard lifecycle.** `@app_env.on_startup` runs before traffic is served, and the
  FastAPI shutdown path replaces signal handling.
- **Health and restarts are handled.** The platform probes the app and restarts it,
  using the endpoint the app already serves.

### Why pull rather than push

Pub/Sub push authenticates with a Google-issued OIDC token and cannot send a Union
credential, so a push receiver must verify Google tokens itself on a public endpoint.
Pull avoids this: the subscriber authenticates outbound to both Google and Union.

Pull also gives flow control. `max_messages` bounds work in flight, where a push
subscription delivers as fast as it can and the receiver absorbs it. When a backlog
drains, that decides whether you launch a controlled number of runs or a flood.

---

## 2. Configuration

### 2.1 Pub/Sub

| Setting | Recommendation |
|---|---|
| Subscription type | pull |
| Ack deadline | 60s — the client extends it while the callback runs |
| Message retention | 7 days |
| Dead-letter topic | required |
| Max delivery attempts | 5 |
| `max_messages` | sized against tolerable concurrent runs |

For a GCS source, filter server-side by object prefix and event type so unrelated bucket
activity never reaches the subscriber.

### 2.2 IAM

Four grants needed:

| Principal | Role | On | Why |
|---|---|---|---|
| GCS service agent | `pubsub.publisher` | the topic | without it the notification config exists but nothing is delivered |
| Pub/Sub service agent | `pubsub.publisher` | dead-letter topic | without it messages retry forever instead of dead-lettering |
| Pub/Sub service agent | `pubsub.subscriber` | the subscription | same |
| The subscriber's Google identity | `pubsub.subscriber` | the subscription | reading messages |

Scope grants to the specific topic or subscription rather than the project.

### 2.3 Credentials

The app needs two, in opposite directions.

**Union.** Store an API key as a Flyte secret; the app reads it from
`FLYTE_API_KEY`. Mint it from a service identity scoped to the target project and
domain rather than a personal account, since the key inherits the permissions of
whoever created it. Rotate on a schedule.

**Google.** Pub/Sub credentials reach the app through Application Default Credentials.

Create the secret before deploying. If it is missing, the pod is rejected with
`none of the secret managers injected secret` and the app fails to start.

### 2.4 App settings

Two settings matter:

- `scaling=Scaling(replicas=(1, 1))`. Apps default to scale-to-zero and autoscale on
  request volume. A subscriber serves no requests, so a default-scaled app is scaled
  away and stops reading the subscription.
- A listener on the app port. The platform health-checks it, and `app.py` serves
  `/health` reporting whether the stream is still running.

### 2.5 Which task version runs

The subscriber should name a release label, never a code version, and should not resolve
"latest" — that makes every deploy an immediate production change with no way to pin or
roll back.

Deploy each release twice: once under an immutable tag that is a permanent record, and
once under a label the subscriber pins.

```bash
flyte deploy --version 2026-09-08-a3f9c21  gcs_ingest.py env   # immutable record
flyte deploy --version prod                gcs_ingest.py env   # what production runs
```

Releases and rollbacks then happen on the Flyte side and the app never changes. Use
meaningful versions — a git SHA or release tag — since auto-generated versions are
content hashes and awkward to promote by hand.

---

## 3. Reliability

These are properties of Pub/Sub, and they decide how the integration behaves under load
and failure.

**Delivery is at-least-once.** Duplicates will happen. Derive the run name from the
Pub/Sub `messageId`: run names are unique per project and domain, so a redelivery
collides with the existing run instead of starting a second one. Treat "already exists"
as success. This holds across replicas, because uniqueness is enforced by the platform
rather than by subscriber state.

**Ack when the run is created, not when it finishes.** Blocking on completion exhausts
the ack deadline and triggers redelivery, which launches duplicates.

**Dead-letter what cannot be processed.** A message that never parses will otherwise
retry indefinitely. Ack permanently-bad messages rather than nacking them, and attach a
dead-letter topic for the rest.

**Ordering keys order delivery, not completion.** Two runs launched in order can finish
out of order. If a pipeline must not run concurrently for a given key, enforce that in
Flyte rather than at the transport.

**Retries have two owners.** Flyte task retries and Pub/Sub redelivery will otherwise
compete. Ack on successful launch, let Flyte own execution retries, and reserve the
dead-letter queue for messages that never launched.

---

## 4. Mapping messages to task inputs

The only bespoke code. Decoding the message body and passing it straight to the task
works when you control the publisher and shaped the payload to match the task signature.
It does not work for cloud-generated events.

GCS object notifications put routing data in `attributes` and the object *resource* in
`data`:

```
attributes:  bucketId, objectId, eventType, eventTime
data:        base64 JSON — {kind, id, selfLink, name, bucket, generation,
                            size, md5Hash, contentType, ...}
```

That payload describes the file; it is not task inputs. Passing it as keyword arguments
sends `kind`, `selfLink` and `md5Hash` to the task and fails. Read `attributes` instead:

```python
inputs = {"object_key": f"gs://{attrs['bucketId']}/{attrs['objectId']}"}
```

Filter on `eventType` as well. A bucket configured for several event types delivers
deletes and metadata updates through the same subscription.

---

## 5. Observability

**Log every decision.** An app that logs nothing looks the same whether it is working or
stalled. Log on launch with the run name and URL, on duplicate delivery, on skip, and on
failure.

**Alert on the subscription, not the app.**

| Signal | Catches |
|---|---|
| `num_undelivered_messages` (backlog) | subscriber down or too slow |
| oldest unacked message age | connected but stalled |
| dead-letter topic depth | messages that never launched a run |
| app restarts | crash loop, which otherwise resembles backlog growth |

**Health should reflect the stream.** A StreamingPull subscriber can stall while the
process stays alive. `/health` returns 503 when the stream is no longer running, so the
platform sees the difference.

---

## 6. Try it yourself

Around 30-45 minutes. Substitute your own project, bucket, and subscription.

Prerequisites: a Union instance with a project and domain, and the `flyte` CLI
configured (`flyte create config --endpoint dns:///<your-endpoint>`).

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
are correct. Everything after this is application code. Leave it unacked and the app
will consume it on first start.

**5. Deploy the task**

```bash
flyte deploy --version r1 gcs_ingest.py env
```

**6. Mint the Union API key and store it**

`flyte create api-key` ships in the `flyteplugins-union` package rather than the base
CLI.

```bash
uv add --dev flyteplugins-union
uv run flyte create api-key --name pubsub-subscriber-key
```

The output is shown once. Store it as the secret the app expects:

```bash
flyte create secret flyte-api-key --value '<the key>'
```

**7. Deploy the app**

Set `GCP_PROJECT` and `SUBSCRIPTION` in `app.py`, then:

```bash
flyte serve app.py app_env
```

The logs should show the app authenticating and subscribing:

```
authenticated to <project>/<domain>
subscribed to projects/<gcp-project>/subscriptions/trainingdata-uploads-sub
```

**8. Trigger it**

```bash
gcloud storage cp file.txt gs://YOUR_BUCKET/trainingdata/file-$(date +%s).txt
```

A run appears in the Flyte console within seconds.

---

## 7. Open questions

To size this for your environment:

1.What creates the messages? 
2. What is the message volume and burst profile, and is it one message per run?
2. How do you decide which code version production runs, and do you need to roll back
   without redeploying?
3. Should runs be attributed to the originating user or tenant? The app calls Flyte with
   its own credentials, so that identity has to travel in the message and be enforced in
   the pipeline.

---

## Appendix: callers that are not Python

Flyte 2's control plane is served over Connect RPC, which accepts `POST` + JSON over
HTTP/1.1 — no protobuf toolchain or code generation.

```
POST /cloudidl.workflow.RunService/CreateRun
Authorization: Bearer <token>
```

The difficulty is not the transport but the inputs: task inputs are typed protobuf
literals, so an integer is `{"scalar": {"primitive": {"integer": "42"}}}` and files are
more involved. Two ways around it — give the task a single string input carrying the raw
message and parse inside the task, or put a thin Python service in front. The first is
usually right.
