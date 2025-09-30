import { LocalDBEntity, LocalDBEntityWithId, LocalDBIndexableType, LocalDBItterator } from './interfaces'
import { LocalDBIndex } from './LocalDBIndex'
import { SharedMutex } from '@david.uhlir/mutex'
import { LevelDB } from './DB/LevelDB'

/**
 *
 * Index getter
 *
 */
export class LocalDBIndexGetter<T extends LocalDBEntity> {
  constructor(private readonly baseKey: string, private db: LevelDB<string, LocalDBEntityWithId<T>>, readonly index: LocalDBIndex<T, any>) {}

  public async exists(value: any): Promise<boolean> {
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.index.getIndexName()}`, async () => {
      return this.index.exists(value)
    })
  }

  public async get(value: any): Promise<LocalDBEntityWithId<T>[]> {
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.index.getIndexName()}`, async () => {
      const foundIds = await this.index.get(value)
      return this.db.getMany(foundIds)
    })
  }

  public async getOne(value: any): Promise<LocalDBEntityWithId<T> | null> {
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.index.getIndexName()}`, async () => {
      const ids = await this.index.get(value)
      if (!ids || !ids.length) {
        return null
      }
      return this.db.get(ids[0])
    })
  }

  public async query(options: LocalDBItterator<LocalDBIndexableType>): Promise<LocalDBEntityWithId<T>[]> {
    return SharedMutex.lockMultiAccess(`${this.baseKey}/index/${this.index.getIndexName()}`, async () => {
      const ids = await this.index.queryItterator(options)
      return this.db.getMany(ids)
    })
  }
}
