# Faikin Home Assistant add-ons

Add-on repository for the Faikin debug Home Assistant at mqtt.faikin.au.

Add it in Settings > Add-ons > Add-on store > (menu) > Repositories with the URL
`https://github.com/andi-christ/ha-addons`.

| Add-on | Purpose |
|---|---|
| [mqtt-mcp](mqtt-mcp/) | MCP server that lets Claude watch and publish on the Mosquitto add-on's broker |

No secrets live in this repository. The MCP endpoint's secret is generated on
the server at first start and only ever appears in the add-on log.
