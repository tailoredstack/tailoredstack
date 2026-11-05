export class UnsupportedError extends Error {
  constructor(feature: string, store: string) {
    super(`${feature} is not supported by ${store}`);
    this.name = "UnsupportedError";
  }
}
