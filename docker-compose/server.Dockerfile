FROM --platform=linux/arm64 golang:latest as builder

WORKDIR /build

COPY ./go.mod /build/go.mod
COPY ./go.sum /build/go.sum

RUN go mod download

COPY . .

RUN GOARCH=arm64 CGO_ENABLED=1 GOOS=linux go build  -ldflags "-linkmode 'external' -extldflags '-static'" -o /bin/server ./cmd/server

FROM scratch

COPY --from=builder /bin/server /bin/server

ENTRYPOINT ["/bin/server"]
