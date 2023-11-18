FROM jupyter/scipy-notebook:python-3.10
USER root
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "cython<3" && \
    pip install --no-build-isolation pyyaml==5.4.1 && \
    pip install --no-cache-dir sqlsynthgen
USER jovyan