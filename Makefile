include .env

build-streamlit:
	docker build -t steramlit:latest ./streamlit/app

build-dagster:
	docker build -t dagster:latest ./dockerimages/dagster

build-xml-pipeline:
	docker build -t elt_pipeline:latest ./elt_pipeline

build-log-pipeline:
	docker build -t log_elt_pipeline:latest ./log_elt_pipeline