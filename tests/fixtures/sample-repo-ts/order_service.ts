import { computeTotal } from './util';
class OrderService {
  minItems: number;
  constructor(minItems: number) { this.minItems = minItems; }
  process(prices: number[]): number {
    this.validate(prices);
    return computeTotal(prices);
  }
  validate(prices: number[]): void {
    if (prices.length < this.minItems) { throw new Error("too few items"); }
  }
}
