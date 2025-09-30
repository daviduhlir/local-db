import { promises as fs } from 'fs'
import * as path from 'path'
import { createRandomId, hashString } from '../utils'
import { SharedMutex } from '@david.uhlir/mutex'
import { JsonDBEntity, JsonDBEntityWithId, JsonDBIdType } from '../interfaces'
import { friendMethodsSymbolAddItem, friendMethodsSymbolClearIndex, friendMethodsSymbolRemapIndex, friendMethodsSymbolRemoveItem, JsonDBIndex } from './JsonDBIndex'
import { FILES } from '../constants'

export interface JsonDBOptionsIndexes {
  [key: string]: {
    path: string
  }
}

export interface JsonDBOptions<I extends JsonDBOptionsIndexes> {
  baseKey?: string
  indexes?: I
}

export class JsonDBRepository<T extends JsonDBEntity, I extends JsonDBOptionsIndexes> {
  protected unique: string
  private indexes: Record<string, JsonDBIndex<T, any>> = {}

  constructor(readonly dbPath: string, readonly options: JsonDBOptions<I> = {}) {
    this.unique = options.baseKey || hashString(dbPath)
  }

  public async open() {
    await fs.mkdir(this.dbPath, { recursive: true })
    if (this.options.indexes) {
      for (const [indexName, indexDef] of Object.entries(this.options.indexes)) {
        this.indexes[indexName] = new JsonDBIndex<T, any>(this.unique, this.dbPath, indexDef.path, {
          getOne: (id) => this.getOneRaw(id),
          getAll: () => this.getAllRaw(),
          get: (ids) => this.getRaw(ids),
        })
        await this.indexes[indexName].open()
      }
    }
  }

  public async close() {
    for (const index of Object.values(this.indexes)) {
      await index.close()
    }
  }

  public getIndex(indexName: keyof I): JsonDBIndex<T> {
    return this.indexes[indexName as string]
  }

  async exists(id: JsonDBIdType): Promise<boolean> {
    return SharedMutex.lockMultiAccess(`${this.unique}/${id}`, async () => {
      return !!await fs.stat(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id))).catch(() => false)
    })
  }

  async getOne(id: JsonDBIdType): Promise<JsonDBEntityWithId<T> | null> {
    return SharedMutex.lockMultiAccess(`${this.unique}/${id}`, async () => {
      return this.getOneRaw(id)
    })
  }

  async get(ids: JsonDBIdType[]): Promise<JsonDBEntityWithId<T>[]> {
    return SharedMutex.lockMultiAccess(`${this.unique}`, async () => {
      return this.getRaw(ids)
    })
  }

  async insert(value: T) {
    return SharedMutex.lockSingleAccess(`${this.unique}/insert`, async () => {
      const id = createRandomId()
      const item = { ...value, $id: id }
      await fs.writeFile(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id)), JSON.stringify(item, null, 2))
      for (const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolAddItem](item)
      }
      return id
    })
  }

  async edit(id: JsonDBIdType, value: T) {
    return SharedMutex.lockSingleAccess(`${this.unique}/${id}`, async () => {
      if (!await fs.stat(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id))).catch(() => false)) {
        throw new Error('Item not found')
      }
      const data = await fs.readFile(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id)), 'utf8')
      try {
        const oldData = JSON.parse(data)
        try {
          const item = { ...oldData, ...value, $id: id }
          await fs.writeFile(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id)), JSON.stringify(item, null, 2))
          for (const index of Object.values(this.indexes)) {
            await index[friendMethodsSymbolRemoveItem](id)
            await index[friendMethodsSymbolAddItem](item)
          }
        } catch (e) {
          throw new Error(`Error writing entity ${id}`)
        }
      } catch (e) {
        throw new Error(`Error parsing entity ${id}`)
      }
    })
  }

  async delete(id: JsonDBIdType) {
    return SharedMutex.lockSingleAccess(`${this.unique}/${id}`, async () => {
      if (!await fs.stat(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id))).catch(() => false)) {
        throw new Error('Item not found')
      }
      await fs.unlink(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id)))
      for (const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolRemoveItem](id)
      }
    })
  }

  async clear() {
    return SharedMutex.lockSingleAccess(`${this.unique}`, async () => {
      const files = await fs.readdir(this.dbPath)
      for (const file of files) {
        if (file.endsWith('.json')) {
          await fs.unlink(path.join(this.dbPath, file))
        }
      }
      for (const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolClearIndex]()
      }
    })
  }

  async getAll(): Promise<JsonDBEntityWithId<T>[]> {
    return SharedMutex.lockMultiAccess(`${this.unique}`, async () => {
      return this.getAllRaw()
    })
  }

  async remapIndex() {
    return SharedMutex.lockSingleAccess(`${this.unique}`, async () => {
      for (const index of Object.values(this.indexes)) {
        await index[friendMethodsSymbolRemapIndex]()
      }
    })
  }

  /**
   *
   * Internal helpers
   *
   */
  protected async getOneRaw(id: JsonDBIdType): Promise<JsonDBEntityWithId<T> | null> {
    try {
      if (!await fs.stat(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id))).catch(() => false)) {
        return null
      }
      const data = await fs.readFile(path.join(this.dbPath, FILES.ENTITY_DB.replace('{id}', id)), 'utf8')
      return JSON.parse(data)
    } catch (e) {
      throw new Error(`Error reading entity ${id}, mixed operations`)
    }
  }

  protected async getRaw(ids: JsonDBIdType[]): Promise<JsonDBEntityWithId<T>[]> {
    try {
      const results: JsonDBEntityWithId<T>[] = []
      for (const id of ids) {
        const data = await this.getOneRaw(id)
        if (data) {
          results.push(data)
        }
      }
      return results
    } catch (e) {
      throw new Error(`Error reading entities ${ids.join(', ')}, mixed operations`)
    }
  }

  protected async getAllRaw(): Promise<JsonDBEntityWithId<T>[]> {
    try {
      const files = await fs.readdir(this.dbPath)
      const results: JsonDBEntityWithId<T>[] = []
      for (const file of files) {
        if (file.endsWith('.json') && !file.startsWith('index-')) {
          const data = await fs.readFile(path.join(this.dbPath, file), 'utf8')
          results.push(JSON.parse(data))
        }
      }
      return results
    } catch (e) {
      throw new Error(`Error reading all entities, mixed operations`)
    }
  }
}

