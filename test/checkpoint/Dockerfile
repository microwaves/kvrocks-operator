FROM apache/kvrocks:2.17.0
COPY --chmod=0555 checkpoint.test /checkpoint.test
ENTRYPOINT ["/checkpoint.test", "-test.v", "-test.timeout=5m"]
