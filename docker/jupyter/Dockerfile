FROM jupyter/scipy-notebook:python-3.10
USER root
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir sqlsynthgen && \
    pip install --no-cache-dir sdv && \
    pip install --no-cache-dir psycopg2-binary && \
    pip install --no-cache-dir black
USER jovyan