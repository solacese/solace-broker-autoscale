# Broker 0 LVQ browser

This helper is the narrow official-JCSMP bridge used to read retained membership snapshots without consuming them. It accepts one JSON request per line on standard input and emits one JSON response per line on standard output.

Required environment variables:

- `SWLB_CONTROL_HOST`
- `SWLB_CONTROL_VPN`
- `SWLB_CONTROL_BROWSER_USERNAME`
- `SWLB_CONTROL_BROWSER_PASSWORD`

The command-line argument is the managed deployment namespace. Only queues beginning with `<namespace>.` are accepted; the Go adapter supplies the exact group-to-queue mapping rather than deriving queue names.

```bash
mvn package
java -jar target/lvq-browser-1.0.0-SNAPSHOT-all.jar example
```

Request:

```json
{"operation":"browse_latest","queue":"swlb.example.membership.flight-operations","timeoutMs":10000}
```

The response payload is Base64 encoded. The helper never acknowledges, settles, or removes browsed messages; it only closes the browser flow. Do not pass credentials on the command line or include them in logs. Requests are limited to 16 KiB, each payload to 1 MiB, and each browse to 16 messages.
