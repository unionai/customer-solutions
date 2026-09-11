"""Artifact-driven chaining: a task publishes, a trigger runs the next task.

Use this when the event originates inside Flyte. A task publishes a new version of a
named artifact, and any task with an `OnArtifact` trigger on that name runs
automatically with the artifact as an input.

Compared with observing an external event source, this removes the subscriber
process, its credentials, and all delivery semantics — no acks, no dead-letter
queue, no idempotency handling. Union does the work.

It does not help when data arrives from a system outside Flyte. Nothing publishes the
artifact in that case, so nothing fires. Use the Pub/Sub subscriber (`app.py`) there.

Verified end to end: running `produce` published a new artifact version, and the
platform launched `consume` on its own with `relation_type: RELATION_TYPE_TRIGGERED`.

    flyte deploy --version r1 artifact_chain.py env
    flyte run --version r1 artifact_chain.py produce --rows 3
"""

from datetime import datetime, timezone

import flyte
from flyte.io import File
from flyte.remote import Artifact

env = flyte.TaskEnvironment(name="artifact_chain")

ARTIFACT = "incoming_dataset"

# Fires on every new version of the artifact. `TriggeredArtifact` binds the artifact
# that fired the trigger to a task input, as `TriggerTime` does for schedules.
on_new_data = flyte.Trigger(
    name="on_new_dataset",
    automation=flyte.OnArtifact(name=ARTIFACT),
    inputs={"dataset": flyte.TriggeredArtifact},
)


@env.task
async def produce(rows: int = 3) -> str:
    """Write a file and publish it as a new version of the artifact."""
    path = "/tmp/dataset.csv"
    with open(path, "w") as fh:
        fh.write("id,value\n")
        for i in range(rows):
            fh.write(f"{i},{i * 10}\n")

    f = await File.from_local(path)

    # `external_ref` is required when publishing from inside a task. Without it the
    # SDK derives provenance from the running action but omits org, project and
    # domain, and the server rejects the request:
    #   spec.source.task_action.action.run.org: must be at least 1 characters
    art = await Artifact.create.aio(
        f,
        name=ARTIFACT,
        description="Dataset produced by the upstream task",
        external_ref=f.path,
        attrs={"rows": str(rows), "at": datetime.now(timezone.utc).isoformat()},
    )
    return f"published {art.name}:{art.version}"


@env.task(triggers=[on_new_data])
async def consume(dataset: File) -> str:
    """Runs once per new version of the artifact. Nothing launches this directly."""
    return f"consumed {dataset.path} on task version {flyte.ctx().version}"


if __name__ == "__main__":
    flyte.init_from_config()
    print(flyte.deploy(env))
