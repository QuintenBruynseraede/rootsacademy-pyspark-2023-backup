ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.2.0
ARG JUPYTER_VERSION=6.5.2

RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
RUN pip --no-cache-dir install notebook==${JUPYTER_VERSION}

# Copy notebook and start server
RUN useradd -ms /bin/bash nonroot
USER nonroot
WORKDIR /home/nonroot

COPY notebooks/pyspark-101-notebook-clean.ipynb .
COPY data data/
CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--port", "8888", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]
EXPOSE 8888/tcp