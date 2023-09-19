FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}

COPY lambda_function.py ${LAMBDA_TASK_ROOT}

RUN pip install --no-cache-dir --upgrade -r requirements.txt

CMD ["lambda_function.handler"]