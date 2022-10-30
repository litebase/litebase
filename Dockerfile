FROM golang:1.19 as builder
WORKDIR /usr/src/app
COPY . .
RUN GOOS=linux GOARCH=arm64 go build -o ./build/bootstrap ./runtime 
FROM scratch
COPY --from=builder /usr/src/app/build/bootstrap /bootstrap 
