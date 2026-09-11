# BuildKit Template Build Internals

This page describes the internal worker, cache, publication, and recovery
lifecycle behind `aenv build`. For the user-facing command and configuration,
see [Create Your Template](../concepts/templates/creating.md#aenv-build).

## Worker Preparation and Reuse

The server provisions and releases an internal worker for each Dockerfile build.
The first build prepares a reusable builder snapshot in a private namespace of
the configured snapshot repository. Nodes sharing that repository restore the
same builder and attach a new cache volume before starting BuildKit.

Concurrent first requests on one node share initialization. Simultaneous first
builds on different nodes may both prepare a builder; subsequent builds reuse
the published snapshot. Builder image, CPU, memory, virtualization mode, and
readiness setup identify that snapshot, so changing those inputs prepares a new
one. Workers are internal and do not appear in public sandbox listings or
endpoints.

## Build Cache Lifecycle

Each build clones the latest immutable cache seed into its own writable volume.
Sequential builds inherit the preceding build's instruction cache and
`RUN --mount=type=cache` data. Concurrent builds can fork the same seed without
waiting; their additions are not merged, and the last successfully published
cache becomes the next seed.

After image import, the node stops BuildKit, checkpoints and publishes its cache
volume through the normal volume capture path, and stops the VM without saving
its memory, rootfs, or device state. A failed shutdown or cache capture keeps the
previous shared seed. Volume ownership remains held until the VM stops.

Cache publication uploads only missing layers. Failure to publish the cache does
not fail an otherwise successful template build or replace the previous seed.
Old cache volumes are removed after active children release their leases. Cache
head updates and pending retirements are committed together, while cleanup
retries retirements independently. BuildKit's garbage collector manages the
cache contents.

## Image Publication

The node reads the completed image directly from the builder by SHA-256 digest,
verifies its bytes, and converts only missing layers by reusing the
content-addressed OverlayBD cache used for registry imports. The image does not
travel through the CLI. Only the final image configuration becomes template
configuration; intermediate stages and build arguments do not become template
environment variables.

Registry credentials come from the local BuildKit session and normal Docker
credential configuration. Cache sharing stays within the snapshot repository's
existing API-key trust boundary.

## Cancellation and Recovery

Cancellation and deadlines release the worker and discard its incomplete cache
child. If the client disappears, the build deadline eventually triggers the
same cleanup. Server restart recovers unfinished builds and cache reservations
from the durable journal. Failed cleanup is retained and retried while the
server runs. Active builds prevent template deletion until cancellation or
completion, and an unreadable journal entry does not block recovery of other
builds or prevent server startup.
