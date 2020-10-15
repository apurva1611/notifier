# build stage
FROM golang AS builder

ENV GO111MODULE=on

WORKDIR /app/

COPY notifier-app/go.mod .
COPY notifier-app/go.sum .

RUN go mod download

COPY notifier-app/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o notifier-app

# final stage
FROM scratch
COPY --from=builder app/notifier-app /app/
ENTRYPOINT ["/app/notifier-app"]