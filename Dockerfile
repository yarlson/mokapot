FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /mokapot ./cmd/mokapot

FROM alpine:3.21
RUN adduser -D -H appuser
COPY --from=build /mokapot /mokapot
RUN mkdir -p /data && chown appuser /data
USER appuser
EXPOSE 4566
ENTRYPOINT ["/mokapot"]
