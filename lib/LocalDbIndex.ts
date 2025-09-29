import { Level } from 'level'
import { FILES } from './constants'
import * as path from 'path'
import { LocalDbEntity, LocalDbEntityWithId, LocalDbIdType, LocalDBIndexableType } from './interfaces'
import { getByExpression } from '@david.uhlir/expression'
import { hashString } from './utils'
import * as charwise from 'charwise'

export const friendMethodsSymbolAddItem = Symbol('addItem')
export const friendMethodsSymbolRemapIndex = Symbol('remapIndex')
export const friendMethodsSymbolRemoveItem = Symbol('removeItem')

export class LocalDbIndex<T extends LocalDbEntity, K extends LocalDBIndexableType> {
  private dbForward: Level<string, { ids: LocalDbIdType[] }>
  private dbBackward: Level<LocalDbIdType, string>
  private indexName: string

  constructor(dbPath: string, readonly indexKeyPath: string, readonly indexKeyType: 'string' | 'number') {
    this.indexName = hashString(indexKeyPath)
    const dbFilenameForward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'forward')
    const dbFilenameBackward = FILES.INDEX_DB.replace('{indexName}', this.indexName).replace('{orientation}', 'backward')
    this.dbForward = new Level(path.join(dbPath, dbFilenameForward), { valueEncoding: 'json' })
    this.dbBackward = new Level(path.join(dbPath, dbFilenameBackward), { valueEncoding: 'json' })
  }

  public async exists(value: K): Promise<boolean> {
    return !!(await this.dbForward.get(LocalDbIndex.serializeKey(value, this.indexKeyType)))?.ids?.length
  }

  public async get(value: K): Promise<LocalDbIdType[] | null> {
    return (await this.dbForward.get(LocalDbIndex.serializeKey(value, this.indexKeyType)))?.ids || []
  }

  public async [friendMethodsSymbolAddItem](item: LocalDbEntityWithId<T>) {
    // TODO make it atomic!
    const value = getByExpression(item, this.indexKeyPath)
    const k = LocalDbIndex.serializeKey(value, this.indexKeyType)
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
      const k = LocalDbIndex.serializeKey(value, this.indexKeyType);
      const ids = (await this.dbForward.get(k))?.ids || [];
      ids.push(item.$id);
      await this.dbForward.put(k, { ids });
      await this.dbBackward.put(item.$id, k);
    }
  }

  protected static serializeKey(value: any, type: 'string' | 'number'): string {
    if (type === 'string') {
      return value
    }
    if (type === 'number') {
      return charwise.encode(10)
    }
    throw new Error('Unknown index key type')
  }
}
