const isPlainObject = (v: object): boolean => {
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
};

/**
 * Throws TypeError if `value` contains something JSON cannot represent: undefined, functions, symbols or
 * bigint, at the top level or anywhere inside plain objects and arrays. Date, Map and Set are not rejected;
 * their lossy JSON form is a documented limit.
 */
export function assertSerializable(value: unknown, path = "value", seen = new Set<object>()): void {
  const type = typeof value;
  if (type === "undefined") throw new TypeError("Cannot cache undefined; use delete() or return null");
  if (type === "function" || type === "symbol" || type === "bigint") {
    throw new TypeError(`Cannot cache a ${type} at ${path}`);
  }
  if (value === null || type !== "object") return;
  const obj = value as object;
  if (!Array.isArray(obj) && !isPlainObject(obj)) return;
  // Cycles: JSON.stringify rejects them on write; here we only stop recursing.
  if (seen.has(obj)) return;
  seen.add(obj);
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) assertSerializable(obj[i], `${path}[${i}]`, seen);
  } else {
    for (const [k, item] of Object.entries(obj)) assertSerializable(item, `${path}.${k}`, seen);
  }
}
