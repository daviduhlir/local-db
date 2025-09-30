import { IteratorOptions, Level } from 'level'
import { FILES } from './constants'
import * as path from 'path'
import { LocalDBEntity, LocalDBEntityWithId, LocalDBIdType, LocalDBIndexableType, LocalDBItterator } from './interfaces'
import { getByExpression } from '@david.uhlir/expression'
import { hashString, levelDbAsyncIterable } from './utils'
import * as charwise from 'charwise'
import { SharedMutex } from '@david.uhlir/mutex'
import { promises as fs } from 'fs'
import { LevelDBCluster, LevelDB } from './DB/LevelDB'

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
  private dbForward: LevelDB<string, LocalDBIndexItem>
  private dbBackward: LevelDB<LocalDBIdType, string>
  private indexName: string
  constructor(protected baseKey: string, readonly dbPath: string, readonly indexKeyPath: string) {
    this.indexName = hashString(indexKeyPath)
  }

  public async open(parentDb: LevelDB<string, LocalDBEntityWithId<T>>) {
    const dbFilenameForward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'forward')
    const dbFilenameBackward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'backward')
    const forwardPath = path.join(this.dbPath, dbFilenameForward)
    const backwardPath = path.join(this.dbPath, dbFilenameBackward)

    const needsRemap = !(await fs.stat(forwardPath).catch(() => false)) || !(await fs.stat(backwardPath).catch(() => false))
    if (needsRemap) {
      await fs.unlink(forwardPath).catch(() => {})
      await fs.unlink(backwardPath).catch(() => {})
    }

    await fs.mkdir(this.dbPath, { recursive: true })
    this.dbForward = await LevelDBCluster.getInstance(this.baseKey + this.indexName + 'forward', forwardPath)
    this.dbBackward = await LevelDBCluster.getInstance(this.baseKey + this.indexName + 'backward',backwardPath)

    if (needsRemap) {
      await this[friendMethodsSymbolRemapIndex](await parentDb.getAll())
    }
  }

  public async close() {
    await this.dbForward.close()
    await this.dbBackward.close()
  }

  public getIndexName() {
    return this.indexName
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
      ne?: LocalDBIndexableType
      eq?: LocalDBIndexableType
    } = {}
    if (itteratorOptions.hasOwnProperty('ne')) {
      customOptions.ne = LocalDBIndex.serializeKeyByValue(itteratorOptions.ne)
    }
    if (itteratorOptions.hasOwnProperty('eq')) {
      customOptions.eq = LocalDBIndex.serializeKeyByValue(itteratorOptions.eq)
    }

    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.indexName}`, async () => {
      const resultsRaw = await this.dbForward.query(usedItteratorOptions, customOptions)
      const results: LocalDBIdType[] = []
      for await (const value of resultsRaw) {
        results.push(...value.ids)
      }
      return results
    })
  }

  public async exists(value: K): Promise<boolean> {
    const k = LocalDBIndex.serializeKeyByValue(value)
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.indexName}/${k}`, async () => {
      return !!(await this.dbForward.get(k))?.ids?.length
    })
  }

  public async get(value: K): Promise<LocalDBIdType[] | null> {
    const k = LocalDBIndex.serializeKeyByValue(value)
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.indexName}/${k}`, async () => {
      return (await this.dbForward.get(k))?.ids || []
    })
  }

  public async [friendMethodsSymbolAddItem](item: LocalDBEntityWithId<T>) {
    // TODO make it atomic!
    const value = getByExpression(item, this.indexKeyPath)
    const k = LocalDBIndex.serializeKeyByValue(value)
    return SharedMutex.lockSingleAccess(`${this.baseKey}/index/${this.indexName}/${k}`, async () => {
      const ids = (await this.dbForward.get(k))?.ids || []
      if (!ids.includes(item.$id)) {
        ids.push(item.$id)
        await this.dbForward.put(k, { ids })
        await this.dbBackward.put(item.$id, k)
      }
    })
  }

  public async [friendMethodsSymbolRemoveItem](id: LocalDBIdType) {
    const k = await this.dbBackward.get(id)
    return SharedMutex.lockSingleAccess(`${this.baseKey}/index/${this.indexName}/${k}`, async () => {
      const ids = (await this.dbForward.get(k))?.ids || []
      const idx = ids.indexOf(id)
      if (idx === -1) {
        return
      }
      ids.splice(idx, 1)
      await this.dbForward.put(k, { ids })
      await this.dbBackward.del(id)
    })
  }

  public async [friendMethodsSymbolRemapIndex](items: [string, LocalDBEntityWithId<T>][]) {
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.indexName}`, async () => {
      // Remap whole index from AsyncIterable
      await this.dbForward.clear()
      await this.dbBackward.clear()
      for (const item of items) {
        const value = getByExpression(item[1], this.indexKeyPath)
        const k = LocalDBIndex.serializeKeyByValue(value)
        const ids = (await this.dbForward.get(k))?.ids || []
        ids.push(item[0])
        await this.dbForward.put(k, { ids })
        await this.dbBackward.put(item[0], k)
      }
    })
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
