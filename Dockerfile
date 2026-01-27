# Copyright (c) 2025 Gecosistema S.r.l.

#FROM ghcr.io/osgeo/gdal:ubuntu-small-3.7.0
FROM 901702069075.dkr.ecr.us-east-1.amazonaws.com/docker-gdal

COPY src /var/tmp/process_meteoblue_hub/src
COPY pyproject.toml /var/tmp/process_meteoblue_hub/
WORKDIR /var/tmp/process_meteoblue_hub 
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
# These following lines are for the AWS Lambda and should be set on the AWS Lambda function on aws web console
# or using aws lambda update-function-configuration --function-name <function-name> --handler <handler-name>
# ENTRYPOINT [ "/opt/venv/bin/python", "-m", "awslambdaric" ] for Ubuntu
# ENTRYPOINT [ "/lambda-entrypoint.sh" ] for awslinux2 and Ubuntu
# CMD [ "lambda_function.lambda_handler" ]
CMD ["bash"]
