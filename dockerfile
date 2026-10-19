FROM apache/airflow:2.9.1

USER root

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar pacotes Python
RUN pip install --no-cache-dir \
    astronomer-cosmos==1.10.1 \
    apache-airflow-providers-google==10.10.0 \
    faker==40.38.0 \
    pandas==2.1.4 \
    google-cloud-storage==2.16.0 \
    requests==2.31.0 \
    dbt-core==1.12.4 \
    dbt-bigquery==1.12.0