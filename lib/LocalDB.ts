import { Level, IteratorOptions } from 'level'
import { LocalDbEntity, LocalDbEntityWithId, LocalDbIdType } from './interfaces'
import { FILES } from './constants'
import * as path from 'path'
import { createRandomId, levelDbAsyncIterable } from './utils'
import { friendMethodsSymbolAddItem, friendMethodsSymbolRemapIndex, friendMethodsSymbolRemoveItem, LocalDbIndex } from './LocalDbIndex'

export interface LocalDBOptionsIndexes {
  [key: string]: {
    path: string
    type: 'string' | 'number'
  }
}

export interface LocalDBOptions<I extends LocalDBOptionsIndexes> {
  indexes?: I
}

export class LocalDBIndexGetter<T extends LocalDbEntity> {
  constructor( private db: LocalDB<T, any>, readonly index: LocalDbIndex<T, any>) {}

  public async exists(value: any): Promise<boolean> {
    return this.index.exists(value)
  }

  public async get(value: any): Promise<LocalDbEntityWithId<T>[]> {
    const foundIds = await this.index.get(value)
    return this.db.get(foundIds)
  }

  public async getOne(value: any): Promise<LocalDbEntityWithId<T> | null> {
    const ids = await this.index.get(value)
    if (!ids || !ids.length) {
      return null
    }
    return this.db.getOne(ids[0])
  }
}

export class LocalDB<T extends LocalDbEntity, I extends LocalDBOptionsIndexes> {
  private db: Level<string, LocalDbEntityWithId<T>>
  private indexes: Record<string, LocalDbIndex<T, any>> = {}
  private indexGetters: Record<string, LocalDBIndexGetter<T>> = {}

  constructor(dbPath: string, options: LocalDBOptions<I> = {}) {
    this.db = new Level(path.join(dbPath, FILES.DATA_DB), { valueEncoding: 'json' })
    if (options.indexes) {
      for (const [indexName, indexDef] of Object.entries(options.indexes)) {
        this.indexes[indexName] = new LocalDbIndex<T, any>(dbPath, indexDef.path, indexDef.type)
        this.indexGetters[indexName] = new LocalDBIndexGetter(this, this.indexes[indexName])
      }
    }
  }

  public getIndex(indexName: keyof I): LocalDBIndexGetter<T> {
    return this.indexGetters[indexName as string]
  }

  async exists(id: LocalDbIdType): Promise<boolean> {
    try {
      const data = await this.db.get(id)
      return typeof data === 'object' && data !== null
    } catch (e) {
      return false
    }
  }

  async getOne(id: string): Promise<LocalDbEntityWithId<T> | null> {
    const data = await this.db.get(id)
    return (typeof data === 'object' && data !== null) ? { ...data, $id: id } : null
  }

  async get(ids: string[]): Promise<LocalDbEntityWithId<T>[]> {
    return this.db.getMany(ids)
  }

  async queryItterator(itteratorOptions: IteratorOptions<string, LocalDbEntityWithId<T>>): Promise<LocalDbEntityWithId<T>[]> {
    const results = []
    for await (const [key, value] of this.db.iterator(itteratorOptions)) {
      results.push(value)
    }
    return results
  }

  async insert(data: T): Promise<LocalDbIdType> {
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
    const oldData = await this.getOne(id)
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
  }

  async delete(id: string): Promise<void> {
    await this.db.del(id)
    for(const index of Object.values(this.indexes)) {
      await index[friendMethodsSymbolRemoveItem](id)
    }
  }

  async remapIndex(): Promise<void> {
    for(const index of Object.values(this.indexes)) {
      await index[friendMethodsSymbolRemapIndex](levelDbAsyncIterable(this.db))
    }
  }
}
