# 1.1.0

- `shared_secrets`: named extra URLs for other people, each revocable on its own. NGINX now gates on exact `/<secret>/mcp` paths and the server listens on a plain `/mcp` behind it.

# 1.0.0

- First release. Rewrite of the mqtt-mcp container that ran on the old faikin-debug VM, as a Home Assistant add-on.
