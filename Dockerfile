# Sử dụng image Airflow chính thức làm base
FROM apache/airflow:2.8.0

# Chuyển sang user root để cài đặt các gói
USER root

# Cài đặt các phụ thuộc hệ thống cần thiết cho dbt (nếu cần)
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Chuyển về user airflow
USER airflow

# Cài đặt dbt qua pip
# Thay đổi adapter theo database bạn sử dụng (ví dụ: dbt-postgres, dbt-snowflake, v.v.)
RUN pip install --no-cache-dir dbt-core dbt-postgres

# Cài đặt Airflow provider cho MySQL
RUN pip install apache-airflow-providers-mysql

# Sao chép các file cấu hình hoặc dự án dbt (nếu có)
COPY ./dbt_project /opt/airflow/dbt_project