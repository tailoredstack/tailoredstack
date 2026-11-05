/**
 * Serializes `value` to the exact JSON-compatible value that will be stored, in one walk. Each `toJSON()` runs
 * once, and what is returned is what gets checked and stored.
 *
 * Throws TypeError for undefined, functions, symbols, bigint, a toJSON() that returns undefined, a circular
 * structure, and undefined or function fields of an object or array (JSON would drop or null them silently).
 *
 * Limits: Date, Map and Set take their JSON form (Date through toJSON, Map and Set become {}). Class private
 * fields (#x) are not own enumerable properties, so they are not stored.
 */
export function toStorable(value: unknown, path = "value"): unknown {
  return walk(value, path, new Set<object>(), true);
}

function walk(value: unknown, path: string, stack: Set<object>, allowToJSON: boolean): unknown {
  const type = typeof value;
  if (type === "undefined") throw new TypeError(`Cannot cache undefined at ${path}; use delete() or return null`);
  if (type === "function" || type === "symbol" || type === "bigint") {
    throw new TypeError(`Cannot cache a ${type} at ${path}`);
  }
  if (value === null || type !== "object") return value;

  const obj = value as Record<string, unknown>;
  if (stack.has(obj)) throw new TypeError(`Cannot cache a circular structure at ${path}`);

  if (allowToJSON && typeof obj.toJSON === "function") {
    // JSON stores the result of toJSON(), not the object, so that result is what must be checked.
    const out: unknown = (obj.toJSON as () => unknown)();
    if (out === undefined) throw new TypeError(`${path} serializes to undefined`);
    // The returned value is serialized without calling its own toJSON again, as JSON does.
    return walk(out, path, stack, false);
  }

  stack.add(obj);
  try {
    if (Array.isArray(obj)) return obj.map((item, i) => walk(item, `${path}[${i}]`, stack, true));
    // fromEntries defines properties rather than assigning, so a "__proto__" key stays data.
    return Object.fromEntries(Object.entries(obj).map(([k, item]) => [k, walk(item, `${path}.${k}`, stack, true)]));
  } finally {
    stack.delete(obj);
  }
}
