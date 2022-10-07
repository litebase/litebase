FROM public.ecr.aws/lambda/go:1
COPY ./lambda.go ${LAMBDA_TASK_ROOT}
CMD [ "lambda" ]
