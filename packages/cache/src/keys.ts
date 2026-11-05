export type Segment = string | number | boolean | null | readonly Segment[] | { readonly [field: string]: Segment };
export type Key = readonly Segment[];
export type KeyInput = string | Key;

export function normalizeKey(input: KeyInput): Key {
  const key = typeof input === "string" ? [input] : input;
  for (const segment of key) assertSegment(segment);
  return key;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function assertSegment(value: unknown): asserts value is Segment {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (Number.isFinite(value)) return;
    throw new TypeError(`Invalid cache key segment: ${value}`);
  }
  if (Array.isArray(value)) {
    for (const item of value) assertSegment(item); // for-of visits holes as undefined
    return;
  }
  if (isPlainObject(value)) {
    for (const field of Object.values(value)) assertSegment(field);
    return;
  }
  throw new TypeError(`Invalid cache key segment of type ${value === undefined ? "undefined" : typeof value}`);
}

// Canonical JSON: object keys sorted, so field order never changes the encoded key.
function canonical(value: Segment): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, Segment>;
    const fields = Object.keys(record)
      .sort()
      .map((field) => `${JSON.stringify(field)}:${canonical(record[field])}`);
    return `{${fields.join(",")}}`;
  }
  return JSON.stringify(value);
}

/** Storage key for a Cache key. Reversible via decodeKey, so invalidate needs no envelope. */
export function encodeKey(key: Key): string {
  for (const segment of key) assertSegment(segment);
  return canonical(key);
}

export function decodeKey(stored: string): Key | null {
  try {
    const parsed: unknown = JSON.parse(stored);
    return Array.isArray(parsed) ? (parsed as Key) : null;
  } catch {
    return null;
  }
}

function partialMatch(filter: unknown, key: unknown): boolean {
  if (filter === key) return true;
  if (Array.isArray(filter)) {
    if (!Array.isArray(key) || filter.length > key.length) return false;
    return filter.every((item, i) => partialMatch(item, key[i]));
  }
  if (isPlainObject(filter)) {
    if (!isPlainObject(key)) return false;
    return Object.keys(filter).every((field) => partialMatch(filter[field], key[field]));
  }
  return false;
}

/** TanStack semantics: partial by default, full equality with `exact`. */
export function matchKey(filter: Key, key: Key, { exact = false }: { exact?: boolean } = {}): boolean {
  return exact ? canonical(filter) === canonical(key) : partialMatch(filter, key);
}
