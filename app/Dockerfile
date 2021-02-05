# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.9-slim

# Add project data for getting secrets
ENV PROJECT_ID=oval-turbine-303415 \
    SECRET_ID=postgres_uri \
    VERSION_ID=latest \
    SUPERSET_REDIRECT_URL=http://localhost:9999/superset/dashboard/1/

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN pip install Flask gunicorn flask-sqlalchemy psycopg2-binary google-cloud-secret-manager

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
