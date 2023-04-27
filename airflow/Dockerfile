FROM apache/airflow:2.3.0
COPY requirements.txt /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
