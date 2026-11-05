/**
 * Throws TypeError if `value` would not survive JSON: undefined, functions, symbols or bigint, a toJSON() that
 * returns undefined, or any of those inside an object or array. Class instances are checked by their own fields,
 * or by what their toJSON() returns, so a field JSON would silently drop is still an error. Date, Map and Set are
 * not rejected; their lossy JSON form is a documented limit.
 */
export function assertSerializable(value: unknown, path = "value", seen = new Set<object>()): void {
  const type = typeof value;
  if (type === "undefined") throw new TypeError("Cannot cache undefined; use delete() or return null");
  if (type === "function" || type === "symbol" || type === "bigint") {
    throw new TypeError(`Cannot cache a ${type} at ${path}`);
  }
  if (value === null || type !== "object") return;
  const obj = value as Record<string, unknown>;
  // Cycles: JSON.stringify rejects them on write; here we only stop recursing.
  if (seen.has(obj)) return;
  seen.add(obj);
  if (typeof obj.toJSON === "function") {
    // JSON stores what toJSON returns, so that is what must be checked.
    const out: unknown = (obj.toJSON as () => unknown)();
    if (out === undefined) throw new TypeError(`${path} serializes to undefined`);
    assertSerializable(out, path, seen);
    return;
  }
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) assertSerializable(obj[i], `${path}[${i}]`, seen);
  } else {
    for (const [k, item] of Object.entries(obj)) assertSerializable(item, `${path}.${k}`, seen);
  }
}
