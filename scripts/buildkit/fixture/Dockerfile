FROM docker.io/library/alpine:3.22 AS build
WORKDIR /src
ARG CACHE_KEY=fixture
RUN --mount=type=cache,id=agentenv-test-${CACHE_KEY},target=/compiler-cache \
    test ! -e /compiler-cache/seed && echo warm > /compiler-cache/seed
COPY . .
ARG BUILD_DELAY=0
RUN --mount=type=cache,id=agentenv-test-${CACHE_KEY},target=/compiler-cache \
    sleep "$BUILD_DELAY" && \
    test ! -e ignored.txt && \
    test "$(cat /compiler-cache/seed)" = warm && \
    mkdir /out && cp input.txt /out/result.txt

FROM docker.io/library/alpine:3.22 AS release
COPY --from=build /out/result.txt /result.txt
ENV BUILD_BACKEND=buildkit
HEALTHCHECK CMD test -s /result.txt && test -s /started
CMD ["/bin/sh", "-c", "echo started >/started; exec sleep infinity"]
