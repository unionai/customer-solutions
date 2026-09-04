"""Task for the GCS-notification variant of the demo.

`hello_driver` takes `ids: list[int]`, which nothing in a GCS object notification maps
onto. This one takes the object that triggered it.

    flyte deploy --version r1 gcs_ingest.py env
"""

from datetime import datetime, timezone

import flyte

env = flyte.TaskEnvironment(name="gcs_ingest")

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@env.task
async def on_object(object_key: str = "", event_time: datetime = EPOCH) -> str:
    """Stand-in for real processing of a newly uploaded object."""
    ctx = flyte.ctx()
    return (
        f"processing {object_key} (uploaded {event_time.isoformat()}) "
        f"on task version {ctx.version}"
    )
