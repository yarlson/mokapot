FROM busybox:1.36.1-musl AS prep
RUN mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=prep --chown=nonroot:nonroot /data /data
ARG TARGETPLATFORM
COPY --chown=nonroot:nonroot $TARGETPLATFORM/mokapot /usr/bin/mokapot
EXPOSE 4566
ENTRYPOINT ["/usr/bin/mokapot"]
