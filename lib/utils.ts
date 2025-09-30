import { createHash, randomBytes } from 'crypto'
export function createRandomId() {
  return randomBytes(16).toString('hex')
}

export function hashString(input: string): string {
  return createHash('sha256').update(input).digest('hex')
}
