# Copyright (c) 2025 Gecosistema S.r.l.

#FROM ghcr.io/osgeo/gdal:ubuntu-small-3.7.0
FROM ubuntu:24.04

COPY src /var/tmp/process_meteoblue_hub/src
COPY pyproject.toml /var/tmp/process_meteoblue_hub/

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-venv \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/tmp/process_meteoblue_hub 
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip install --upgrade pip setuptools wheel
RUN pip install .
ADD tests /var/task/tests

#Clean up
RUN pip cache purge
RUN apt-get remove -y git && \
    apt-get autoremove -y && \
    apt-get clean
RUN rm -rf /var/tmp/process_meteoblue_hub/

# AWS Lambda
# copy the entrypoint script to use it like awslinux2
RUN pip install awslambdaric
COPY lambda-entrypoint.sh /lambda-entrypoint.sh
RUN chmod +x /lambda-entrypoint.sh

COPY ./lambda/* /var/task/
WORKDIR /var/task

# Dual-mode processor configuration
# METEOBLUE_PROCESSOR_MODE: "local" (default) or "lambda"
# - "local": Run logic locally in the processor (backward compatible, writes to cwd)
# - "lambda": Invoke Lambda function for processing (writes only to /tmp)
ENV METEOBLUE_PROCESSOR_MODE=lambda
ENV AWS_REGION=us-east-1

# These following lines are for the AWS Lambda and should be set on the AWS Lambda function on aws web console
# or using aws lambda update-function-configuration --function-name <function-name> --handler <handler-name>
# ENTRYPOINT [ "/opt/venv/bin/python", "-m", "awslambdaric" ] for Ubuntu
# ENTRYPOINT [ "/lambda-entrypoint.sh" ] for awslinux2 and Ubuntu
# CMD [ "lambda_function.lambda_handler" ]
CMD ["bash"]
