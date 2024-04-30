FROM --platform=linux/arm64 golang:latest as builder

WORKDIR /build

COPY ./go.mod /build/go.mod
COPY ./go.sum /build/go.sum

RUN go mod download

COPY . .

RUN GOARCH=arm64 CGO_ENABLED=0 GOOS=linux go build -o /bin/router ./cmd/router

FROM --platform=linux/arm64 alpine:latest

COPY --from=builder /bin/router /bin/router

ENTRYPOINT ["/bin/router"]
