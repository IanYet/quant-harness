export class WsChannel {
  private socket?: WebSocket;
  private candidate?: WebSocket;
  private replacing?: ReturnType<typeof setTimeout>;
  private reconnect?: ReturnType<typeof setTimeout>;
  private streams = new Set<string>();
  private active = new Set<string>();
  private counter = 0;
  private attempt = 0;
  private closed = false;

  constructor(private readonly url: string, private readonly onFrame: (stream: string, data: unknown) => void,
    private readonly onReady?: () => void, private readonly onDown?: () => void) {}

  set(streams: Iterable<string>) {
    if (this.closed) return;
    this.streams = new Set(streams);
    if (!this.streams.size) {
      this.sync();
      this.disconnect();
      return;
    }
    this.sync();
    if (!this.socket && !this.candidate && !this.reconnect) this.connect();
  }

  private sync() {
    if (this.socket?.readyState !== WebSocket.OPEN) return;
    const add = [...this.streams].filter(stream => !this.active.has(stream));
    const del = [...this.active].filter(stream => !this.streams.has(stream));
    for (const [method, params] of [["UNSUBSCRIBE", del], ["SUBSCRIBE", add]] as const) {
      if (params.length) this.socket.send(JSON.stringify({ method, params, id: ++this.counter }));
    }
    this.active = new Set(this.streams);
  }

  private connect() {
    if (this.closed || !this.streams.size || this.candidate) return;
    const next = new WebSocket(this.url);
    this.candidate = next;
    const timeout = setTimeout(() => next.close(), 10_000);
    next.onopen = () => {
      clearTimeout(timeout);
      if (this.candidate !== next) return next.close();
      const old = this.socket;
      this.socket = next;
      this.candidate = undefined;
      this.active.clear();
      this.attempt = 0;
      this.sync();
      old?.close();
      clearTimeout(this.replacing);
      this.replacing = setTimeout(() => this.connect(), 23 * 3600_000 + 50 * 60_000);
      this.onReady?.();
    };
    next.onmessage = message => {
      if (this.socket !== next || typeof message.data !== "string") return;
      try {
        const frame = JSON.parse(message.data) as { stream?: string; data?: unknown; code?: number };
        if (frame.code !== undefined) {
          console.warn("Binance WS subscription rejected", this.url, frame.code);
          this.attempt = Math.max(this.attempt, 2);
          next.close();
        } else if (frame.stream && this.streams.has(frame.stream)) this.onFrame(frame.stream, frame.data);
      } catch (error) { console.warn("Invalid Binance market frame", (error as Error).message); }
    };
    next.onerror = () => next.close();
    next.onclose = () => {
      clearTimeout(timeout);
      const ready = this.socket === next;
      if (this.candidate === next) this.candidate = undefined;
      else if (!ready) return;
      if (ready) {
        this.socket = undefined;
        this.onDown?.();
      }
      if (!this.closed && this.streams.size) {
        const delay = [1000, 2000, 5000, 15_000][Math.min(this.attempt++, 3)];
        clearTimeout(this.reconnect);
        this.reconnect = setTimeout(() => { this.reconnect = undefined; this.connect(); }, delay);
      }
    };
  }

  private disconnect() {
    clearTimeout(this.reconnect);
    clearTimeout(this.replacing);
    this.reconnect = this.replacing = undefined;
    const sockets = [this.socket, this.candidate];
    this.socket = this.candidate = undefined;
    this.active.clear();
    for (const socket of sockets) socket?.close();
  }

  close() {
    this.closed = true;
    this.streams.clear();
    this.disconnect();
  }
}
