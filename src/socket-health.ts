// Bun exposes ping/pong control frames on its WebSocket client.
export function monitorSocket(ws: WebSocket, report: (at: number) => void) {
  const socket = ws as unknown as Bun.WebSocket;
  let lastReply = Date.now();
  socket.addEventListener("pong", () => {
    lastReply = Date.now();
    report(lastReply);
  });
  const timer = setInterval(() => {
    if (socket.readyState !== WebSocket.OPEN) return;
    if (Date.now() - lastReply > 65000) socket.terminate();
    else socket.ping();
  }, 30000);
  socket.addEventListener("close", () => clearInterval(timer), { once: true });
  socket.ping();
}
