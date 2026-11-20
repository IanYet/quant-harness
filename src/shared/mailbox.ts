export class Mailbox<T> {
  private readonly queue: T[] = [];
  private waiter?: (value: T | undefined) => void;
  private closed = false;
  dropped = 0;

  constructor(private readonly capacity: number, private readonly dropOldest = false) {}

  offer(item: T) {
    if (this.closed) return false;
    if (this.waiter) {
      const resolve = this.waiter;
      this.waiter = undefined;
      resolve(item);
      return true;
    }
    if (this.queue.length === this.capacity) {
      if (!this.dropOldest) { this.close(); return false; }
      this.queue.shift();
      this.dropped++;
    }
    this.queue.push(item);
    return true;
  }

  take(): Promise<T | undefined> {
    if (this.queue.length || this.closed) return Promise.resolve(this.queue.shift());
    return new Promise(resolve => { this.waiter = resolve; });
  }

  close() {
    this.closed = true;
    this.queue.length = 0;
    this.waiter?.(undefined);
    this.waiter = undefined;
  }
}
