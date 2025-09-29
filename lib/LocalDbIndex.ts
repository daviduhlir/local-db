import { IteratorOptions, Level } from 'level'
import { FILES } from './constants'
import * as path from 'path'
import { LocalDbEntity, LocalDbEntityWithId, LocalDbIdType, LocalDBIndexableType } from './interfaces'
import { getByExpression } from '@david.uhlir/expression'
import { hashString } from './utils'
import * as charwise from 'charwise'

export const friendMethodsSymbolAddItem = Symbol('addItem')
export const friendMethodsSymbolRemapIndex = Symbol('remapIndex')
export const friendMethodsSymbolRemoveItem = Symbol('removeItem')

export type LocalDbIndexItem = { ids: LocalDbIdType[] }

export interface LocalDbItterator<T = any> {
  gt?: T
  gte?: T
  lt?: T
  lte?: T
  eq?: T
  ne?: T
}

/**
 *
 * Index getter
 *
 */
export class LocalDBIndexGetter<T extends LocalDbEntity> {
  constructor( private db: Level<string, LocalDbEntityWithId<T>>, readonly index: LocalDbIndex<T, any>) {}

  public async exists(value: any): Promise<boolean> {
    return this.index.exists(value)
  }

  public async get(value: any): Promise<LocalDbEntityWithId<T>[]> {
    const foundIds = await this.index.get(value)
    return this.db.getMany(foundIds)
  }

  public async getOne(value: any): Promise<LocalDbEntityWithId<T> | null> {
    const ids = await this.index.get(value)
    if (!ids || !ids.length) {
      return null
    }
    return this.db.get(ids[0])
  }

  public async query(options: LocalDbItterator<LocalDBIndexableType>): Promise<LocalDbEntityWithId<T>[]> {
    const ids = await this.index.queryItterator(options)
    return this.db.getMany(ids)
  }
}

/**
 *
 * Index
 *
 * DB Index
 */

export class LocalDbIndex<T extends LocalDbEntity, K extends LocalDBIndexableType> {
  private dbForward: Level<string, LocalDbIndexItem>
  private dbBackward: Level<LocalDbIdType, string>
  private indexName: string

  constructor(dbPath: string, readonly indexKeyPath: string) {
    this.indexName = hashString(indexKeyPath)
    const dbFilenameForward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'forward')
    const dbFilenameBackward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'backward')
    this.dbForward = new Level(path.join(dbPath, dbFilenameForward), { valueEncoding: 'json' })
    this.dbBackward = new Level(path.join(dbPath, dbFilenameBackward), { valueEncoding: 'json' })
  }

  async queryItterator(itteratorOptions: LocalDbItterator<LocalDBIndexableType>): Promise<LocalDbIdType[]> {
    const usedItteratorOptions: IteratorOptions<any, LocalDbIndexItem> = {
      keys: true,
      values: true,
    }
    if (itteratorOptions.gt) {
      usedItteratorOptions.gt = LocalDbIndex.serializeKeyByValue(itteratorOptions.gt)
    }
    if (itteratorOptions.gte) {
      usedItteratorOptions.gte = LocalDbIndex.serializeKeyByValue(itteratorOptions.gte)
    }
    if (itteratorOptions.lt) {
      usedItteratorOptions.lt = LocalDbIndex.serializeKeyByValue(itteratorOptions.lt)
    }
    if (itteratorOptions.lte) {
      usedItteratorOptions.lte = LocalDbIndex.serializeKeyByValue(itteratorOptions.lte)
    }

    const customOptions: {
      ne?: LocalDBIndexableType,
      eq?: LocalDBIndexableType,
    } = {}
    if (itteratorOptions.hasOwnProperty('ne')) {
      customOptions.ne = LocalDbIndex.serializeKeyByValue(itteratorOptions.ne)
    }
    if (itteratorOptions.hasOwnProperty('eq')) {
      customOptions.eq = LocalDbIndex.serializeKeyByValue(itteratorOptions.eq)
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
    return !!(await this.dbForward.get(LocalDbIndex.serializeKeyByValue(value)))?.ids?.length
  }

  public async get(value: K): Promise<LocalDbIdType[] | null> {
    return (await this.dbForward.get(LocalDbIndex.serializeKeyByValue(value)))?.ids || []
  }

  public async [friendMethodsSymbolAddItem](item: LocalDbEntityWithId<T>) {
    // TODO make it atomic!
    const value = getByExpression(item, this.indexKeyPath)
    const k = LocalDbIndex.serializeKeyByValue(value)
    const ids = (await this.dbForward.get(k))?.ids || []

    if (!ids.includes(item.$id)) {
      ids.push(item.$id)
      await this.dbForward.put(k, { ids })
      await this.dbBackward.put(item.$id, k)
    }
  }

  public async [friendMethodsSymbolRemoveItem](id: LocalDbIdType) {
    const k = await this.dbBackward.get(id)
    const ids = (await this.dbForward.get(k))?.ids || []
    ids.splice(ids.indexOf(id), 1)
    await this.dbForward.put(k, { ids })
    await this.dbBackward.del(id)
  }

  public async [friendMethodsSymbolRemapIndex](items: AsyncIterable<LocalDbEntityWithId<T>>) {
    // Remap whole index from AsyncIterable
    await this.dbForward.clear();
    await this.dbBackward.clear();
    for await (const item of items) {
      const value = getByExpression(item, this.indexKeyPath);
      const k = LocalDbIndex.serializeKeyByValue(value);
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
      return value.toISOString()
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
