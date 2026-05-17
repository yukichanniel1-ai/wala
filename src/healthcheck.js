/**
 * healthcheck.js — HTTP server on PORT for Railway healthcheck
 * Ported from Python main.py railway heartbeat concept
 */
const http = require('http');

let server = null;

function startHealthcheckServer(port) {
  if (!port) {
    console.log('[HEALTH] No PORT env var — healthcheck server disabled');
    return;
  }

  server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK\n');
  });

  server.listen(port, () => {
    console.log(`[HEALTH] Healthcheck server listening on port ${port}`);
  });

  server.on('error', (e) => {
    console.error(`[HEALTH] Server error: ${e.message}`);
  });
}

function stopHealthcheckServer() {
  if (server) {
    server.close();
    server = null;
  }
}

module.exports = { startHealthcheckServer, stopHealthcheckServer };
