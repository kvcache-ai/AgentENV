FROM docker.io/library/alpine:3.22 AS build
WORKDIR /src
RUN --mount=type=cache,target=/compiler-cache \
    test ! -e /compiler-cache/seed && echo warm > /compiler-cache/seed
COPY . .
RUN --mount=type=cache,target=/compiler-cache \
    test ! -e ignored.txt && \
    test "$(cat /compiler-cache/seed)" = warm && \
    mkdir /out && cp input.txt /out/result.txt

FROM docker.io/library/alpine:3.22 AS release
COPY --from=build /out/result.txt /result.txt
ENV BUILD_BACKEND=buildkit
CMD ["/bin/sh", "-c", "echo started >/started; exec sleep infinity"]
