import { computeTotal } from './util.js';

class OrderService {
  constructor(minItems) { this.minItems = minItems; }
  process(prices) {
    this.validate(prices);
    return computeTotal(prices); // cross-file, risolto da SPEC-025 (T2.5 parte 2/3) via l'import esplicito qui sopra
  }
  validate(prices) {
    if (prices.length < this.minItems) { throw new Error("too few items"); }
  }
}
