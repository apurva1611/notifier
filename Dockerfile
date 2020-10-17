# build stage
FROM golang AS builder

ENV GO111MODULE=on

WORKDIR /app/

COPY notifier/go.mod .
COPY notifier/go.sum .

RUN go mod download

COPY notifier/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o notifier

# final stage
FROM scratch
COPY --from=builder app/notifier /app/
ENTRYPOINT ["/app/notifier"]