FROM apache/airflow:2.6.1

RUN pip install pandas \
    requests \
    apache-airflow-providers-amazon \
    pytz \
    apache-airflow-providers-mongo