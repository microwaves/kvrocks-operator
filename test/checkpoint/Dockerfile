FROM chrislusf/seaweedfs:4.48 AS object-store
FROM apache/kvrocks:2.17.0
COPY --from=object-store /usr/bin/weed /usr/local/bin/weed
COPY --chmod=0555 checkpoint.test /checkpoint.test
ENTRYPOINT ["/checkpoint.test", "-test.v", "-test.timeout=5m"]
