# Production NATS boundary

`nats-production.compose.yaml` is the Part 04 deployment example. It pins NATS
2.10.26, requires mutual TLS, authenticates each process with a public NKey,
encrypts JetStream records with `JS_KEY`, and mounts an externally managed
encrypted volume. Private NKey seeds are supplied only to clients through
`NATS_NKEY_SEED_FILE`; they never enter the server configuration.

The checked-in Compose example contains one exact worker identity. Duplicate
the worker authorization entry for each additional worker, or issue user JWTs
with the same per-worker subject grants through a NATS operator/account
resolver. A worker must never receive wildcard access to another worker's
dispatch, control, or inbox namespace.

Before deployment:

1. Copy the server template to your deployment-secret/config store.
2. Generate distinct API, scheduler, and worker user NKeys. Store only public
   keys in the server environment and mount each private seed into its owning
   client.
3. Supply a high-entropy `JS_KEY`, an encrypted external Docker volume, and a
   server certificate/key plus the client CA used for mutual TLS.
4. For every client, set a `tls://` `NATS_URL`, its own seed file, the CA, and
   its client certificate/key. Do not reuse identities.
5. Validate the rendered configuration with the pinned image before starting:
   `docker run --rm -v <config>:/etc/nats/nats-server.conf:ro nats:2.10.26-alpine -t -c /etc/nats/nats-server.conf`.

The monitoring listener stays on loopback inside the container and is not
published by Compose.
