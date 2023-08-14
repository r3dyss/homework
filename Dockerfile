FROM golang:alpine AS builder
WORKDIR /mnt/homework
COPY . .
RUN go build

# Docker is used as a base image so you can easily start playing around in the container using the Docker command line client.
FROM docker
COPY --from=builder /mnt/homework/homework-object-storage /usr/local/bin/homework-object-storage
RUN apk add bash curl

CMD ["/usr/local/bin/homework-object-storage"]
