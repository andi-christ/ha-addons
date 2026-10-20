ARG BUILD_FROM=python:3.12-slim
FROM ${BUILD_FROM}

# Pinned deliberately: FastMCP 4 changes the HTTP run API. Same pin as the
# other Faikin MCP servers in claude-workspace/mcp.
RUN pip install --no-cache-dir "fastmcp==3.4.6" "paho-mqtt==2.1.0"

WORKDIR /app
COPY server.py .

CMD ["python", "-u", "server.py"]
