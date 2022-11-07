FROM amazonlinux AS builder

RUN yum install golang -y

WORKDIR /build

ENV GOOS=linux
ENV GOARCH=arm64
ENV CGO_ENABLED=1

COPY go.mod /build/go.mod
COPY go.sum /build/go.sum

COPY . /build
RUN --mount=type=cache,target=/root/.cache/go-build \ 
	go build -o bootstrap ./function
FROM scratch
COPY --from=builder /build/bootstrap /bootstrap 
