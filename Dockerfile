FROM apache/airflow:3.3.2-python3.12

# Re-stating apache-airflow pins it, so pip cannot upgrade or downgrade Airflow
# while installing our requirements (recommended way to extend the image).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt
