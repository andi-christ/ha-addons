# MQTT MCP

An MCP server that gives an AI assistant (Claude, via a custom connector) a
view of the Mosquitto broker running on this Home Assistant: live traffic on a
topic filter, retained snapshots, a topic census, publishing with reply
capture, clearing retained messages, `$SYS` statistics and the broker log.

The broker host and login are read from the Supervisor's mqtt service, which the
Mosquitto add-on provides, so no broker user has to be created for it. Set
`mqtt_host` and friends only to point it at a different broker.

## Options

| Option | Default | Meaning |
|---|---|---|
| `secret` | empty | The URL path segment that protects the endpoint. Leave empty to have one generated and stored in the add-on's data; set a new value to rotate it |
| `ssl` | true | Serve HTTPS using the certificate in `/ssl` |
| `certfile`, `keyfile` | `fullchain.pem`, `privkey.pem` | Files in `/ssl` |
| `public_host` | `mqtt.faikin.au` | Only used to print the connector URL in the log |
| `mqtt_host`, `mqtt_port`, `mqtt_user`, `mqtt_password` | empty | Override the Supervisor-provided broker |
| `max_window_seconds` | 60 | Longest live window for `watch` and `topic_tree` |
| `max_messages` | 500 | Most messages a single call returns |
| `payload_limit` | 2000 | Characters of payload returned before truncation |

## Connecting Claude

Start the add-on and read the line `MCP endpoint: https://<host>:8087/<secret>/mcp`
from its log. In Claude, Settings > Connectors > Add custom connector, paste that
URL, leave the OAuth fields blank, Add, then Connect. The URL is the credential:
anyone with it can publish on the broker.

Port 8087 has to be reachable from the internet (a firewall rule on the host) and
the certificate in `/ssl` has to match the hostname in the URL. The add-on reads
the certificate at start, so restart it after a renewal.

## Rotating the secret

Set `secret` to a new value (or clear it and delete `/data/secret.txt` by
reinstalling), restart the add-on, then delete and re-add the connector in
Claude.
