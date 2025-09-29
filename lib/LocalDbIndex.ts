import { IteratorOptions, Level } from 'level'
import { FILES } from './constants'
import * as path from 'path'
import { LocalDBEntity, LocalDBEntityWithId, LocalDBIdType, LocalDBIndexableType, LocalDBItterator } from './interfaces'
import { getByExpression } from '@david.uhlir/expression'
import { hashString } from './utils'
import * as charwise from 'charwise'

export const friendMethodsSymbolAddItem = Symbol('addItem')
export const friendMethodsSymbolRemapIndex = Symbol('remapIndex')
export const friendMethodsSymbolRemoveItem = Symbol('removeItem')

export type LocalDBIndexItem = { ids: LocalDBIdType[] }

/**
 *
 * Index
 *
 * DB Index
 */

export class LocalDBIndex<T extends LocalDBEntity, K extends LocalDBIndexableType> {
  private dbForward: Level<string, LocalDBIndexItem>
  private dbBackward: Level<LocalDBIdType, string>
  private indexName: string

  constructor(private readonly baseKey: string, dbPath: string, readonly indexKeyPath: string) {
    this.indexName = hashString(indexKeyPath)
    const dbFilenameForward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'forward')
    const dbFilenameBackward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'backward')
    this.dbForward = new Level(path.join(dbPath, dbFilenameForward), { valueEncoding: 'json' })
    this.dbBackward = new Level(path.join(dbPath, dbFilenameBackward), { valueEncoding: 'json' })
  }

  async queryItterator(itteratorOptions: LocalDBItterator<LocalDBIndexableType>): Promise<LocalDBIdType[]> {
    const usedItteratorOptions: IteratorOptions<any, LocalDBIndexItem> = {
      keys: true,
      values: true,
    }
    if (itteratorOptions.gt) {
      usedItteratorOptions.gt = LocalDBIndex.serializeKeyByValue(itteratorOptions.gt)
    }
    if (itteratorOptions.gte) {
      usedItteratorOptions.gte = LocalDBIndex.serializeKeyByValue(itteratorOptions.gte)
    }
    if (itteratorOptions.lt) {
      usedItteratorOptions.lt = LocalDBIndex.serializeKeyByValue(itteratorOptions.lt)
    }
    if (itteratorOptions.lte) {
      usedItteratorOptions.lte = LocalDBIndex.serializeKeyByValue(itteratorOptions.lte)
    }

    const customOptions: {
      ne?: LocalDBIndexableType,
      eq?: LocalDBIndexableType,
    } = {}
    if (itteratorOptions.hasOwnProperty('ne')) {
      customOptions.ne = LocalDBIndex.serializeKeyByValue(itteratorOptions.ne)
    }
    if (itteratorOptions.hasOwnProperty('eq')) {
      customOptions.eq = LocalDBIndex.serializeKeyByValue(itteratorOptions.eq)
    }

    const results = []
    for await (const [key, value] of this.dbForward.iterator(usedItteratorOptions)) {
      if (customOptions.eq && key !== customOptions.eq) {
        continue
      }
      if (customOptions.ne && key === customOptions.ne) {
        continue
      }
      results.push(...value.ids)
    }
    return results
  }

  public async exists(value: K): Promise<boolean> {
    return !!(await this.dbForward.get(LocalDBIndex.serializeKeyByValue(value)))?.ids?.length
  }

  public async get(value: K): Promise<LocalDBIdType[] | null> {
    return (await this.dbForward.get(LocalDBIndex.serializeKeyByValue(value)))?.ids || []
  }

  public async [friendMethodsSymbolAddItem](item: LocalDBEntityWithId<T>) {
    // TODO make it atomic!
    const value = getByExpression(item, this.indexKeyPath)
    const k = LocalDBIndex.serializeKeyByValue(value)
    const ids = (await this.dbForward.get(k))?.ids || []

    if (!ids.includes(item.$id)) {
      ids.push(item.$id)
      await this.dbForward.put(k, { ids })
      await this.dbBackward.put(item.$id, k)
    }
  }

  public async [friendMethodsSymbolRemoveItem](id: LocalDBIdType) {
    const k = await this.dbBackward.get(id)
    const ids = (await this.dbForward.get(k))?.ids || []
    const idx = ids.indexOf(id)
    if (idx === -1) {
      return
    }
    ids.splice(idx, 1)
    await this.dbForward.put(k, { ids })
    await this.dbBackward.del(id)
  }

  public async [friendMethodsSymbolRemapIndex](items: AsyncIterable<LocalDBEntityWithId<T>>) {
    // Remap whole index from AsyncIterable
    await this.dbForward.clear();
    await this.dbBackward.clear();
    for await (const item of items) {
      const value = getByExpression(item, this.indexKeyPath);
      const k = LocalDBIndex.serializeKeyByValue(value);
      const ids = (await this.dbForward.get(k))?.ids || [];
      ids.push(item.$id);
      await this.dbForward.put(k, { ids });
      await this.dbBackward.put(item.$id, k);
    }
  }

  protected static serializeKeyByValue(value: any): string {
    if (typeof value === 'undefined') {
      return hashString('undefined')
    }
    if (value instanceof Date) {
      return charwise.encode(value.getTime())
    }
    if (typeof value === 'string') {
      return value
    }
    if (typeof value === 'number') {
      return charwise.encode(value)
    }
    if (typeof value === 'boolean') {
      return value ? '1' : '0'
    }
    if (value === null) {
      return hashString('null')
    }
    if (typeof value === 'object') {
      return hashString(JSON.stringify(value))
    }
    throw new Error('Unknown index key type')
  }
}
