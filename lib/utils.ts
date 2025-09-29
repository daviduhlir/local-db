import { createHash, randomBytes } from 'crypto'
import { Level } from 'level';

export function createRandomId() {
  return randomBytes(16).toString('hex')
}

export function hashString(input: string): string {
  return createHash('sha256').update(input).digest('hex');
}

export async function* levelDbAsyncIterable<T>(db: Level<string, T>): AsyncIterable<T> {
  const iterator = db.iterator();
  while (true) {
    const result = await iterator.next();
    if (result === undefined) break;
    const [key, value] = result;
    yield value;
  }
  await iterator.close();
}