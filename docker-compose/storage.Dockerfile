FROM --platform=linux/arm64 golang:latest as builder

WORKDIR /build

COPY ./go.mod /build/go.mod
COPY ./go.sum /build/go.sum

RUN go mod download

COPY . .

RUN GOARCH=arm64 CGO_ENABLED=0 GOOS=linux go build -o /bin/storage ./cmd/storage

FROM --platform=linux/arm64 public.ecr.aws/lambda/go:latest 

COPY --from=builder /bin/storage /bin/storage

ENV AWS_LAMBDA_FUNCTION_TIMEOUT=3
ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "/bin/storage"]
