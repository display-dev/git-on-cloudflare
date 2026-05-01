/**
 * Generic binary heap implementation with a user-provided comparator.
 *
 * Comparator contract:
 * - cmp(a, b) < 0  => a has higher priority than b (a comes out first)
 * - cmp(a, b) === 0 => a and b are equivalent in priority
 * - cmp(a, b) > 0  => b has higher priority than a
 *
 * Usage:
 *   const heap = new BinaryHeap<number>((a, b) => a - b); // min-heap
 *   heap.push(3); heap.push(1); heap.push(2);
 *   console.log(heap.pop()); // 1
 */
export type Comparator<T> = (a: T, b: T) => number;

export class BinaryHeap<T> {
  private a: T[];
  private cmp: Comparator<T>;

  constructor(cmp: Comparator<T>, items?: Iterable<T>) {
    this.cmp = cmp;
    this.a = [];
    if (items) {
      for (const it of items) this.a.push(it);
      if (this.a.length > 1) this.heapify();
    }
  }

  size(): number {
    return this.a.length;
  }

  isEmpty(): boolean {
    return this.a.length === 0;
  }

  peek(): T | undefined {
    return this.a[0];
  }

  push(v: T): void {
    this.a.push(v);
    this.siftUp(this.a.length - 1);
  }

  pop(): T | undefined {
    if (this.a.length === 0) return undefined;
    const top = this.a[0];
    const last = this.a.pop()!;
    if (this.a.length > 0) {
      this.a[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  private heapify(): void {
    for (let i = Math.floor(this.a.length / 2) - 1; i >= 0; i--) this.siftDown(i);
  }

  private siftDown(i: number): void {
    const n = this.a.length;
    while (true) {
      const l = i * 2 + 1;
      const r = l + 1;
      let best = i;
      if (l < n && this.cmp(this.a[l], this.a[best]) < 0) best = l;
      if (r < n && this.cmp(this.a[r], this.a[best]) < 0) best = r;
      if (best === i) break;
      this.swap(i, best);
      i = best;
    }
  }

  private siftUp(i: number): void {
    while (i > 0) {
      const p = Math.floor((i - 1) / 2);
      if (this.cmp(this.a[i], this.a[p]) < 0) {
        this.swap(i, p);
        i = p;
      } else {
        break;
      }
    }
  }

  private swap(i: number, j: number) {
    const t = this.a[i];
    this.a[i] = this.a[j];
    this.a[j] = t;
  }
}
