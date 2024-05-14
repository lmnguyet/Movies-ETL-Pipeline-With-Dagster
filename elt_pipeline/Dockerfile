FROM python:3.9-slim
WORKDIR /opt/dagster/app
COPY requirements.txt /opt/dagster/app
RUN pip install -r requirements.txt
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "elt_pipeline"]