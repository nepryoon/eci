export function computeTotal(prices: number[]): number {
  return prices.reduce((sum, p) => sum + p, 0);
}
