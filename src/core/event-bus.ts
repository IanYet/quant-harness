import type { Database } from "bun:sqlite";
import { CONSUMER_TOPICS, MARKET_TOPICS, matches } from "../shared/events";
import type { BusEvent, BusFrame } from "../shared/events";
import { Mailbox } from "../shared/mailbox";
import { bad } from "../shared/http";

export type Emit = (topic: string, source: string, payload: Record<string, unknown>, ts?: number) => BusEvent;
type Consumer = keyof typeof CONSUMER_TOPICS;
type Subscription = { topics: readonly string[]; filter?: (event: BusEvent) => boolean; after?: number; queueSize?: number };
const durable = (topic: string) => !topic.startsWith("market.");

export class EventBus {
  private next: number;
  private reserved: number;
  private published: number;
  private readonly ring: Array<BusEvent | undefined>;
  private offset = 0;
  private length = 0;
  private readonly listeners = new Set<(event: BusEvent) => void>();
  private readonly startedAt = Date.now();

  constructor(readonly db: Database, private readonly ringSize = 20_000, private readonly blockSize = 4096) {
    if (ringSize < 1 || blockSize < 1) throw new Error("Invalid bus capacity");
    this.reserved = db.query<{ reserved_until: number }, []>("SELECT reserved_until FROM sequence_state").get()!.reserved_until;
    this.published = this.reserved;
    this.next = this.reserved + 1;
    this.ring = new Array(ringSize);
  }

  get watermark() { return this.published; }
  get ringLength() { return this.length; }

  private reserve(count: number) {
    const needed = this.next + count - 1;
    if (needed <= this.reserved) return;
    const boundary = this.reserved + Math.ceil((needed - this.reserved) / this.blockSize) * this.blockSize;
    if (!Number.isSafeInteger(boundary)) throw new Error("Event sequence exhausted");
    this.db.query("UPDATE sequence_state SET reserved_until=?").run(boundary);
    this.reserved = boundary;
  }

  commit<T>(write: (emit: Emit) => T, capacity = 1): T {
    this.reserve(capacity);
    const frames: BusEvent[] = [];
    const result = this.db.transaction(() => write((topic, source, payload, ts = Date.now()) => {
      if (frames.length >= capacity) throw new Error("Transaction exceeds event reservation");
      const event = { seq: this.next++, ts, topic, source, payload };
      if (durable(topic)) this.db.query("INSERT INTO critical_events VALUES (?,?,?)").run(event.seq, topic, JSON.stringify(event));
      frames.push(event);
      return event;
    })).immediate();
    for (const frame of frames) this.broadcast(frame);
    return result;
  }

  publish(topic: string, source: string, payload: Record<string, unknown>, ts = Date.now()) {
    if (durable(topic)) return this.commit(emit => emit(topic, source, payload, ts));
    this.reserve(1);
    const event = { seq: this.next++, ts, topic, source, payload };
    this.broadcast(event);
    return event;
  }

  private broadcast(event: BusEvent) {
    this.published = event.seq;
    const tail = (this.offset + this.length) % this.ringSize;
    this.ring[tail] = event;
    if (this.length < this.ringSize) this.length++;
    else this.offset = (this.offset + 1) % this.ringSize;
    for (const listener of this.listeners) listener(event);
  }

  private snapshot(after: number) {
    const frames: BusEvent[] = [];
    for (let i = 0; i < this.length; i++) {
      const frame = this.ring[(this.offset + i) % this.ringSize]!;
      if (frame.seq > after) frames.push(frame);
    }
    return frames;
  }

  follow(opts: Subscription) {
    const through = this.published;
    const after = opts.after ?? through;
    if (after > through) bad("after_seq exceeds the published waterline");
    const accepts = (e: BusEvent) => matches(e.topic, opts.topics) && (!opts.filter || opts.filter(e));
    const replay = this.snapshot(after);
    const beforeRing = (replay[0]?.seq ?? through + 1) - 1;
    const queue = new Mailbox<BusFrame>(opts.queueSize ?? 1000);
    const listener = (event: BusEvent) => {
      if (accepts(event) && !queue.offer(event)) close();
    };
    const close = () => {
      this.listeners.delete(listener);
      clearInterval(timer);
      queue.close();
    };
    const timer = setInterval(() => {
      if (!queue.offer({ type: "watermark", throughSeq: this.published })) close();
    }, 5000);
    this.listeners.add(listener);
    const db = this.db;
    const startedAt = this.startedAt;

    async function* frames(): AsyncGenerator<BusFrame> {
      try {
        const topics = MARKET_TOPICS.filter(topic => matches(topic, opts.topics));
        if (beforeRing > after && topics.length) {
          yield { type: "gap", topics, fromSeq: after + 1, toSeq: beforeRing, toTs: replay[0]?.ts ?? Math.max(Date.now(), startedAt) };
        }
        let cursor = after;
        const read = db.query<{ seq: number; envelope: string }, [number, number]>(
          "SELECT seq,envelope FROM critical_events WHERE seq>? AND seq<=? ORDER BY seq LIMIT 200");
        while (cursor < beforeRing) {
          const rows = read.all(cursor, beforeRing);
          if (!rows.length) break;
          for (const row of rows) {
            const e = JSON.parse(row.envelope) as BusEvent;
            if (accepts(e)) yield e;
            cursor = row.seq;
          }
          if (rows.length === 200) await Bun.sleep(0);
        }
        for (const event of replay) if (accepts(event)) yield event;
        yield { type: "watermark", throughSeq: through };
        for (;;) {
          const frame = await queue.take();
          if (frame === undefined) return;
          yield frame;
        }
      } finally {
        close();
      }
    }
    return { frames: frames(), close };
  }

  ack(consumer: Consumer, seq: number) {
    if (seq > this.published) bad("Acknowledgement exceeds published waterline");
    this.db.query("UPDATE consumer_ack SET through_seq=MAX(through_seq,?) WHERE consumer=?").run(seq, consumer);
  }

  prune() {
    this.db.query("DELETE FROM critical_events WHERE seq IN (SELECT seq FROM critical_events WHERE seq<=" +
      "(SELECT MIN(through_seq) FROM consumer_ack) ORDER BY seq LIMIT 2000)").run();
  }

  pending(consumer: Consumer) {
    const { through_seq } = this.db.query<{ through_seq: number }, [string]>("SELECT through_seq FROM consumer_ack WHERE consumer=?").get(consumer)!;
    const durableCount = this.db.query<{ n: number }, [number]>("SELECT COUNT(*) AS n FROM critical_events WHERE seq>?").get(through_seq)!.n;
    return durableCount + this.snapshot(through_seq).filter(event => !durable(event.topic)).length;
  }

  replay(topic: string, after: number, through: number, limit = 1000) {
    return this.db.query<{ envelope: string }, [string, number, number, number]>(
      "SELECT envelope FROM critical_events WHERE topic=? AND seq>? AND seq<=? ORDER BY seq LIMIT ?")
      .all(topic, after, through, limit).map(row => JSON.parse(row.envelope) as BusEvent);
  }
}
