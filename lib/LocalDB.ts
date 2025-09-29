import { Level } from 'level'
import { LocalDBEntity, LocalDBEntityWithId, LocalDBIdType } from './interfaces'
import { FILES } from './constants'
import * as path from 'path'
import { createRandomId, hashString, levelDbAsyncIterable } from './utils'
import { friendMethodsSymbolAddItem, friendMethodsSymbolRemapIndex, friendMethodsSymbolRemoveItem, LocalDBIndex } from './LocalDBIndex'
import { LocalDBIndexGetter } from './LocalDBIndexGetter'
import { SharedMutex } from '@david.uhlir/mutex'

export interface LocalDBOptionsIndexes {
  [key: string]: {
    path: string
  }
}

export interface LocalDBOptions<I extends LocalDBOptionsIndexes> {
  baseKey?: string
  indexes?: I
}

/**
 *
 * Local DB
 *
 *  Main class for local database
 *
 */
export class LocalDB<T extends LocalDBEntity, I extends LocalDBOptionsIndexes> {
  private db: Level<string, LocalDBEntityWithId<T>>
  private indexes: Record<string, LocalDBIndex<T, any>> = {}
  private indexGetters: Record<string, LocalDBIndexGetter<T>> = {}
  private baseKey: string

  constructor(dbPath: string, options: LocalDBOptions<I> = {}) {
    this.baseKey = options.baseKey || hashString(dbPath)
    this.db = new Level(path.join(dbPath, FILES.DATA_DB), { valueEncoding: 'json' })
    if (options.indexes) {
      for (const [indexName, indexDef] of Object.entries(options.indexes)) {
        this.indexes[indexName] = new LocalDBIndex<T, any>(this.baseKey, dbPath, indexDef.path)
        this.indexGetters[indexName] = new LocalDBIndexGetter(this.baseKey, this.db, this.indexes[indexName])
      }
    }
  }

  public async close() {
    await this.db.close()
    for(const index of Object.values(this.indexes)) {
      await index.close()
    }
  }

  public getIndex(indexName: keyof I): LocalDBIndexGetter<T> {
    return this.indexGetters[indexName as string]
  }

  async exists(id: LocalDBIdType): Promise<boolean> {
    try {
      const data = await this.db.get(id)
      return typeof data === 'object' && data !== null
    } catch (e) {
      return false
    }
  }

  async getOne(id: string): Promise<LocalDBEntityWithId<T> | null> {
    const data = await this.db.get(id)
    return (typeof data === 'object' && data !== null) ? { ...data, $id: id } : null
  }

  async get(ids: string[]): Promise<LocalDBEntityWithId<T>[]> {
    if (!ids.length) {
      return []
    }
    return this.db.getMany(ids)
  }

  async insert(data: T): Promise<LocalDBIdType> {
    const id = createRandomId()
    const value = {
      ...data,
      $id: id,
    }
    await this.db.put(id, value)
    for(const index of Object.values(this.indexes)) {
      await index[friendMethodsSymbolAddItem](value)
    }
    return id
  }

  async edit(id: string, data: Partial<T>): Promise<void> {
    return SharedMutex.lockSingleAccess(`${this.baseKey}/${id}`, async () => {
      const oldData = await this.db.get(id)
      if (!oldData) {
        throw new Error('Item not found')
      }
      const newData = {
        ...oldData,
        ...data,
      }
      await this.db.put(id, newData)
      for(const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolRemoveItem](id)
        await index[friendMethodsSymbolAddItem](newData)
      }
    })
  }

  async delete(id: string): Promise<void> {
    return SharedMutex.lockSingleAccess(`${this.baseKey}/${id}`, async () => {
      if (!await this.exists(id)) {
        throw new Error('Item not found')
      }
      await this.db.del(id)
      for(const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolRemoveItem](id)
      }
    })
  }

  async remapIndex(): Promise<void> {
    return SharedMutex.lockSingleAccess(`${this.baseKey}`, async () => {
      for(const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolRemapIndex](levelDbAsyncIterable(this.db))
      }
    })
  }
}
