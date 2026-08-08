function computeTotal(prices) {
  return prices.reduce((sum, p) => sum + p, 0);
}
