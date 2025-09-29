import { Level } from 'level'
import { LocalDBEntity, LocalDBEntityWithId, LocalDBIndexableType, LocalDBItterator } from './interfaces'
import { LocalDBIndex } from './LocalDBIndex'

/**
 *
 * Index getter
 *
 */
export class LocalDBIndexGetter<T extends LocalDBEntity> {
  constructor(private db: Level<string, LocalDBEntityWithId<T>>, readonly index: LocalDBIndex<T, any>) {}

  public async exists(value: any): Promise<boolean> {
    return this.index.exists(value)
  }

  public async get(value: any): Promise<LocalDBEntityWithId<T>[]> {
    const foundIds = await this.index.get(value)
    return this.db.getMany(foundIds)
  }

  public async getOne(value: any): Promise<LocalDBEntityWithId<T> | null> {
    const ids = await this.index.get(value)
    if (!ids || !ids.length) {
      return null
    }
    return this.db.get(ids[0])
  }

  public async query(options: LocalDBItterator<LocalDBIndexableType>): Promise<LocalDBEntityWithId<T>[]> {
    const ids = await this.index.queryItterator(options)
    return this.db.getMany(ids)
  }
}
