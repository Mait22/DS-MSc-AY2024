FROM python:3.9.16-slim

# MS SQL driver install 
RUN apt-get -y update
RUN apt-get -y install curl
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get -y update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

RUN apt-get -y update
RUN apt-get -y install nano
RUN apt-get -y install gcc python3-dev musl-dev postgresql-server-dev-all
RUN apt-get -y update

# Add code to wd
WORKDIR /opt/dagster/app/elt_pipeline  
COPY requirements.txt /opt/dagster/app/elt_pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app/elt_pipeline

# Start dagster service and listen gRPC port 4000
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "elt_pipeline"]
