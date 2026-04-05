FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY automation_script.py features.py lambda_handler.py ./
COPY randomforest-model/ ./randomforest-model/

CMD ["lambda_handler.handler"]
