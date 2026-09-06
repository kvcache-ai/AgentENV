FROM alpine:3.22
ARG RUN_AS=0
USER ${RUN_AS}
CMD ["sh", "-c", "id -u > /tmp/uid && id -g > /tmp/gid && exec sleep 86400"]
HEALTHCHECK CMD ["test", "-s", "/tmp/gid"]
