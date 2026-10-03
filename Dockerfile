FROM python:3.11-slim

WORKDIR /app

# No third-party dependencies: the service runs on the Python stdlib
# only, so the image builds without any network package installs.
COPY app/ ./app/
COPY tests/ ./tests/
COPY scripts/ ./scripts/

RUN python -m compileall -q app tests scripts

EXPOSE 8080

CMD ["python", "-m", "app.server"]
