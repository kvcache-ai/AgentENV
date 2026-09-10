FROM docker.io/library/alpine:3.22 AS build
WORKDIR /src
ARG CACHE_KEY=fixture
RUN --mount=type=cache,id=agentenv-test-${CACHE_KEY},target=/compiler-cache \
    if test ! -f /compiler-cache/seed; then cat /proc/sys/kernel/random/uuid > /compiler-cache/seed; fi
COPY . .
ARG BUILD_DELAY=0
RUN --mount=type=cache,id=agentenv-test-${CACHE_KEY},target=/compiler-cache \
    echo worker-running && sleep "$BUILD_DELAY" && \
    test ! -e ignored.txt && \
    mkdir /out && cp input.txt /out/result.txt && \
    cp /compiler-cache/seed /out/seed && \
    cat /proc/sys/kernel/random/uuid > /out/execution

FROM docker.io/library/alpine:3.22 AS release
COPY --from=build /out/ /
ENV BUILD_BACKEND=buildkit
HEALTHCHECK CMD test -s /result.txt && test -s /started
CMD ["/bin/sh", "-c", "echo started >/started; exec sleep infinity"]
