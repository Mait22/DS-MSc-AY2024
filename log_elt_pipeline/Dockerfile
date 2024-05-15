FROM python:3.9.16-slim

RUN apt-get -y update
RUN apt-get -y install nano
RUN apt-get -y install gcc python3-dev musl-dev postgresql-server-dev-all
RUN apt-get -y update

# Add wd code
WORKDIR /opt/dagster/app/log_elt_pipeline  
COPY requirements.txt /opt/dagster/app/log_elt_pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app/log_elt_pipeline

# NB! This is gRPC endpoint that is used to communicate with Dagster workers and Daemon services 
## i.e. not the TPC port opened in Docker compose configuration file
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4444", "-m", "log_elt_pipeline"]
