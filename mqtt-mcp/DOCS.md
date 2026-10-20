# MQTT MCP

An MCP server that gives an AI assistant (Claude, via a custom connector) a
view of the Mosquitto broker running on this Home Assistant: live traffic on a
topic filter, retained snapshots, a topic census, publishing with reply
capture, clearing retained messages, `$SYS` statistics and the broker log.

The broker host and login are read from the Supervisor's mqtt service, which the
Mosquitto add-on provides, so no broker user has to be created for it. Set
`mqtt_host` and friends only to point it at a different broker.

## How it is reached

By default the add-on sits behind the NGINX SSL proxy add-on. At start it writes
`/share/nginx_proxy_default_mqtt_mcp.conf`, a `location /<secret>/` block that
proxies to the add-on. For the proxy to include it:

1. NGINX SSL proxy add-on, Configuration: `customize` > `active: true` (the
   `default` pattern `nginx_proxy_default*.conf` is the stock value).
2. Restart the NGINX SSL proxy add-on after this add-on's first start, and again
   whenever the secret changes.

The connector URL is then `https://<public_host>/<secret>/mcp`, on port 443 with
the proxy's certificate. No new firewall port is needed.

The alternative is `nginx_include: false`, `ssl: true` and a published port
8087: the add-on then serves HTTPS itself from the certificate in `/ssl`
(restart it after a renewal) and port 8087 has to be opened on the host.

## Options

| Option | Default | Meaning |
|---|---|---|
| `secret` | empty | The URL path segment that protects the endpoint. Leave empty to have one generated and stored in the add-on's data; set a new value to rotate it |
| `nginx_include` | true | Write the location snippet to `/share` for the NGINX SSL proxy add-on |
| `ssl` | false | Serve HTTPS directly using the certificate in `/ssl` |
| `certfile`, `keyfile` | `fullchain.pem`, `privkey.pem` | Files in `/ssl`, for `ssl: true` |
| `public_host` | `mqtt.faikin.au` | Only used to print the connector URL in the log |
| `mqtt_host`, `mqtt_port`, `mqtt_user`, `mqtt_password` | empty | Override the Supervisor-provided broker |
| `max_window_seconds` | 60 | Longest live window for `watch` and `topic_tree` |
| `max_messages` | 500 | Most messages a single call returns |
| `payload_limit` | 2000 | Characters of payload returned before truncation |

## Connecting Claude

Start the add-on and read the line `MCP endpoint via the NGINX proxy: https://<host>/<secret>/mcp`
from its log. In Claude, Settings > Connectors > Add custom connector, paste that
URL, leave the OAuth fields blank, Add, then Connect. The URL is the credential:
anyone with it can publish on the broker.

## Rotating the secret

Set `secret` to a new value, restart this add-on, restart the NGINX SSL proxy
add-on, then delete and re-add the connector in Claude. Clearing `secret` goes
back to the generated one stored in the add-on's data.
