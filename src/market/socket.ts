import { monitorSocket } from "../socket-health";
import type { Config } from "../config";
import { message, wireJSON } from "../util";
import type { FeedEvents, Group } from "./source";

export class MarketSocket {
  state = "disconnected";
  confirmed = new Set<string>();
  lastPong: number | null = null;
  private wanted = new Set<string>();
  private sent = new Set<string>();
  private pending = new Map<number, { add: boolean; keys: string[] }>();
  private id = 0;
  private ws?: WebSocket;
  private retry?: Timer;
  private stopped = false;
  constructor(
    private group: Group,
    private cfg: Config,
    private events: FeedEvents,
  ) {}
  set(keys: Set<string>) {
    this.wanted = keys;
    if (this.ws?.readyState === WebSocket.OPEN) this.sync();
    else if (!this.ws && !this.retry && keys.size) this.open();
  }
  async ready(key: string) {
    const until = Date.now() + this.cfg.timeoutMs;
    while (!this.confirmed.has(key)) {
      if (this.stopped || !this.wanted.has(key) || Date.now() >= until)
        throw new Error(`Subscription unavailable: ${key}`);
      await Bun.sleep(20);
    }
  }
  private sync() {
    for (const add of [false, true]) {
      const keys = add
        ? [...this.wanted].filter((k) => !this.sent.has(k))
        : [...this.sent].filter((k) => !this.wanted.has(k));
      if (!keys.length) continue;
      for (const k of keys) {
        if (add) this.sent.add(k);
        else {
          this.sent.delete(k);
          this.confirmed.delete(k);
        }
      }
      this.pending.set(++this.id, { add, keys });
      this.ws!.send(JSON.stringify({ method: add ? "SUBSCRIBE" : "UNSUBSCRIBE", params: keys, id: this.id }));
    }
  }
  private open() {
    this.state = "connecting";
    const ws = (this.ws = new WebSocket(`${this.cfg.ws}/${this.group}/stream`));
    ws.onopen = () => {
      this.state = "connected";
      monitorSocket(ws, (t) => {
        this.lastPong = t;
      });
      this.sync();
      this.events.up(this.group);
    };
    ws.onmessage = ({ data }) => {
      try {
        const item = wireJSON(String(data));
        if (item.id != null) {
          const op = this.pending.get(item.id);
          this.pending.delete(item.id);
          if (item.result !== null) {
            ws.close();
            return;
          }
          if (op?.add) for (const k of op.keys) if (this.wanted.has(k)) this.confirmed.add(k);
        } else if (this.wanted.has(item.stream) && item.data) {
          this.confirmed.add(item.stream);
          this.events.message(item.stream, item.data);
        }
      } catch (err) {
        console.error("Market frame", message(err));
        ws.close();
      }
    };
    ws.onerror = () => ws.close();
    ws.onclose = () => {
      this.state = "disconnected";
      this.ws = undefined;
      this.sent.clear();
      this.confirmed.clear();
      this.pending.clear();
      if (this.stopped) return;
      this.events.down(this.group, "websocket disconnected");
      this.retry = setTimeout(() => {
        this.retry = undefined;
        if (this.wanted.size) this.open();
      }, this.cfg.reconnectMs);
    };
  }
  stop() {
    this.stopped = true;
    clearTimeout(this.retry);
    this.ws?.close();
  }
}
