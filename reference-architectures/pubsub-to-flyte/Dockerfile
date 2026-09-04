FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Only the subscriber. The pipeline code is not needed in this image — the task is
# fetched by reference (remote.Task.get), so releases happen on the Flyte side and
# this image never changes with them.
COPY subscriber.py .

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin subscriber
USER 10001
# The SDK caches an auth token under $HOME/.flyte; the manifest mounts an emptyDir
# here so this stays writable under readOnlyRootFilesystem.
ENV HOME=/home/subscriber

EXPOSE 8080
CMD ["python", "-u", "subscriber.py"]
