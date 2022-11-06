FROM golang:1.19 as builder
WORKDIR /usr/src/app
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \ 
RUN GOOS=linux GOARCH=arm64 go build -o bootstrap ./function 
FROM scratch
COPY --from=builder /usr/src/app/bootstrap /bootstrap 
