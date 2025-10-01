import { SharedMutex } from '@david.uhlir/mutex'
import { JsonDBEntity, JsonDBEntityWithId, JsonDBIdType, JsonDBIndexableType, JsonDBItterator } from '../interfaces'
import { hashString } from '../utils'
import { promises as fs } from 'fs'
import * as path from 'path'
import { FILES } from '../constants'
import * as charwise from 'charwise'
import { getByExpression } from '@david.uhlir/expression'

export const friendMethodsSymbolAddItem = Symbol('addItem')
export const friendMethodsSymbolRemapIndex = Symbol('remapIndex')
export const friendMethodsSymbolRemoveItem = Symbol('removeItem')
export const friendMethodsSymbolClearIndex = Symbol('clearIndex')

export type JsonDBIndexValue = string
export interface JsonDBIndexedData {
  forward: { [k: JsonDBIndexValue]: JsonDBIdType[] }
  backward: { [k: JsonDBIdType]: JsonDBIndexValue }
}

export class JsonDBIndex<T extends JsonDBEntity, K extends JsonDBIndexableType = JsonDBIndexableType> {
  private indexName: string
  constructor(
    protected unique: string,
    readonly dbPath: string,
    readonly indexKeyPath: string,
    readonly dataGetters: {
      getOne: (id: JsonDBIdType) => Promise<JsonDBEntityWithId<T> | null>
      getAll: () => Promise<JsonDBEntityWithId<T>[]>
      get: (ids: JsonDBIdType[]) => Promise<JsonDBEntityWithId<T>[]>
    },
  ) {
    this.indexName = hashString(indexKeyPath)
  }

  public async open() {
    const indexFile = path.join(this.dbPath, FILES.INDEX_DB.replace('{indexName}', this.indexName))
    if (!(await fs.stat(indexFile).catch(() => false))) {
      await fs.mkdir(this.dbPath, { recursive: true })
      await this[friendMethodsSymbolRemapIndex]()
    }
  }

  public async close() {
    // Nothing to do
  }

  public async exists(value: K): Promise<boolean> {
    return this.readIndex(async data => {
      const k = serializeKeyByValue(value)
      return !!data.forward[k]?.length
    })
  }

  public async get(value: any): Promise<JsonDBEntityWithId<T>[]> {
    const ids = await this.readIndex(async data => {
      const k = serializeKeyByValue(value)
      return data.forward[k] || []
    })
    return SharedMutex.lockMultiAccess(`${this.unique}`, async () => {
      return this.dataGetters.get(ids)
    })
  }

  public async getOne(value: any): Promise<JsonDBEntityWithId<T> | null> {
    const id = await this.readIndex(async data => {
      const k = serializeKeyByValue(value)
      if (!data.forward[k] || !data.forward[k].length) {
        return null
      }
      return data.forward[k][0]
    })
    return SharedMutex.lockMultiAccess(`${this.unique}/${id}`, async () => {
      return this.dataGetters.getOne(id)
    })
  }

  public async query(options: JsonDBItterator<JsonDBIndexableType>): Promise<JsonDBEntityWithId<T>[]> {
    const usedItteratorOptions: JsonDBItterator<JsonDBIndexValue> = {}
    if (options.hasOwnProperty('gt')) {
      usedItteratorOptions.gt = serializeKeyByValue(options.gt)
    }
    if (options.hasOwnProperty('gte')) {
      usedItteratorOptions.gte = serializeKeyByValue(options.gte)
    }
    if (options.hasOwnProperty('lt')) {
      usedItteratorOptions.lt = serializeKeyByValue(options.lt)
    }
    if (options.hasOwnProperty('lte')) {
      usedItteratorOptions.lte = serializeKeyByValue(options.lte)
    }
    if (options.hasOwnProperty('eq')) {
      usedItteratorOptions.eq = serializeKeyByValue(options.eq)
    }
    if (options.hasOwnProperty('ne')) {
      usedItteratorOptions.ne = serializeKeyByValue(options.ne)
    }
    if (options.hasOwnProperty('in')) {
      usedItteratorOptions.in = options.in.map(serializeKeyByValue)
    }
    if (options.hasOwnProperty('nin')) {
      usedItteratorOptions.nin = options.nin.map(serializeKeyByValue)
    }
    if (options.hasOwnProperty('includes')) {
      usedItteratorOptions.includes = serializeKeyByValue(options.includes)
    }
    if (options.hasOwnProperty('excludes')) {
      usedItteratorOptions.excludes = serializeKeyByValue(options.excludes)
    }
    if (options.hasOwnProperty('empty')) {
      usedItteratorOptions.empty = options.empty
    }
    if (options.hasOwnProperty('notEmpty')) {
      usedItteratorOptions.notEmpty = options.notEmpty
    }
    return this.readIndex(async data => {
      const results: JsonDBIdType[] = []
      for (const k of Object.keys(data.forward)) {
        if (usedItteratorOptions.gt && k <= usedItteratorOptions.gt) {
          continue
        }
        if (usedItteratorOptions.gte && k < usedItteratorOptions.gte) {
          continue
        }
        if (usedItteratorOptions.lt && k >= usedItteratorOptions.lt) {
          continue
        }
        if (usedItteratorOptions.lte && k > usedItteratorOptions.lte) {
          continue
        }
        if (usedItteratorOptions.eq && k !== usedItteratorOptions.eq) {
          continue
        }
        if (usedItteratorOptions.ne && k === usedItteratorOptions.ne) {
          continue
        }
        if (usedItteratorOptions.in && !usedItteratorOptions.in.includes(k)) {
          continue
        }
        if (usedItteratorOptions.nin && usedItteratorOptions.nin.includes(k)) {
          continue
        }

        if (
          usedItteratorOptions.hasOwnProperty('includes') ||
          usedItteratorOptions.hasOwnProperty('excludes') ||
          usedItteratorOptions.hasOwnProperty('empty') ||
          usedItteratorOptions.hasOwnProperty('notEmpty')
        ) {
          const ks = k.split(';')
          const isEmpty = ks.length === 1 && ks[0] === ''

          if (usedItteratorOptions.hasOwnProperty('includes') && !ks.includes(usedItteratorOptions.includes)) {
            continue
          }
          if (usedItteratorOptions.hasOwnProperty('excludes') && ks.includes(usedItteratorOptions.excludes)) {
            continue
          }
          if (usedItteratorOptions.hasOwnProperty('empty')) {
            if (usedItteratorOptions.empty && !isEmpty) {
              continue
            }
            if (!usedItteratorOptions.empty && isEmpty) {
              continue
            }
          }
          if (usedItteratorOptions.hasOwnProperty('notEmpty')) {
            if (usedItteratorOptions.notEmpty && isEmpty) {
              continue
            }
            if (!usedItteratorOptions.notEmpty && !isEmpty) {
              continue
            }
          }
        }
        results.push(...data.forward[k])
      }
      return this.dataGetters.get(results)
    })
  }

  /**
   *
   * Friend methods
   *
   */
  public async [friendMethodsSymbolAddItem](item: JsonDBEntityWithId<T>) {
    const value = getByExpression(item, this.indexKeyPath)
    const k = serializeKeyByValue(value)
    return this.modifyIndex(async data => {
      const ids = data.forward[k] || []
      if (!ids.includes(item.$id)) {
        ids.push(item.$id)
        data.forward[k] = ids
        data.backward[item.$id] = k
      }
      return data
    })
  }

  public async [friendMethodsSymbolRemoveItem](id: JsonDBIdType) {
    return this.modifyIndex(async data => {
      const k = data.backward[id]
      if (!k && k !== '') {
        return data
      }
      const ids = data.forward[k] || []
      const idx = ids.indexOf(id)
      if (idx === -1) {
        return data
      }
      ids.splice(idx, 1)
      delete data.backward[id]
      if (ids.length) {
        data.forward[k] = ids
      } else {
        delete data.forward[k]
      }
      return data
    })
  }

  public async [friendMethodsSymbolRemapIndex]() {
    return SharedMutex.lockSingleAccess(`${this.unique}`, async () => {
      const items = await this.dataGetters.getAll()
      return this.modifyIndex(async data => {
        data.forward = {}
        data.backward = {}
        for (const item of items) {
          const value = getByExpression(item, this.indexKeyPath)
          const k = serializeKeyByValue(value)
          const ids = data.forward[k] || []
          ids.push(item.$id)
          data.forward[k] = ids
          data.backward[item.$id] = k
        }
        return data
      })
    })
  }

  public async [friendMethodsSymbolClearIndex]() {
    return this.modifyIndex(async data => {
      data.forward = {}
      data.backward = {}
      return data
    })
  }

  /**
   *
   * Internal methods
   *
   */

  protected async readIndex<T>(getter: (data: JsonDBIndexedData) => Promise<T>): Promise<T> {
    return SharedMutex.lockMultiAccess<T>(`${this.unique}-index:${this.indexName}`, async () => {
      const data = await this.readIndexData()
      return getter(data)
    })
  }

  protected async modifyIndex(modifier: (data: JsonDBIndexedData) => Promise<JsonDBIndexedData>): Promise<void> {
    await SharedMutex.lockSingleAccess(`${this.unique}-index:${this.indexName}`, async () => {
      const data = await this.readIndexData()
      const result = await modifier(data)
      await this.writeIndexData(result)
    })
  }

  protected async readIndexData(): Promise<JsonDBIndexedData> {
    try {
      const indexFile = path.join(this.dbPath, FILES.INDEX_DB.replace('{indexName}', this.indexName))
      if (!(await fs.stat(indexFile).catch(() => false))) {
        return {
          forward: {},
          backward: {},
        }
      }
      return JSON.parse(await fs.readFile(indexFile, 'utf8'))
    } catch (e) {
      throw new Error(`Error reading index ${this.indexName}`)
    }
  }

  protected async writeIndexData(data: JsonDBIndexedData) {
    const indexFile = path.join(this.dbPath, FILES.INDEX_DB.replace('{indexName}', this.indexName))
    await fs.writeFile(indexFile, JSON.stringify(data, null, 2))
  }
}

function serializeKeyByValue(value: any): string {
  if (Array.isArray(value)) {
    return value.map(serializeKeyByValue).join(';')
  }
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
