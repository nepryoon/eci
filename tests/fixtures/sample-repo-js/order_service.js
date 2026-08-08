class OrderService {
  constructor(minItems) { this.minItems = minItems; }
  process(prices) {
    this.validate(prices);
    return computeTotal(prices); // cross-file, deliberatamente non catturato (stesso principio del fixture Go)
  }
  validate(prices) {
    if (prices.length < this.minItems) { throw new Error("too few items"); }
  }
}
