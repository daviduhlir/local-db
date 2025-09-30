import { LocalDBEntity } from './interfaces'
import { LocalDBRepository, LocalDBOptions } from './LocalDBRepository'

export class LocalDBDefinition<T extends LocalDBEntity> {
  constructor(readonly options: LocalDBOptions<any>) {}
  declare __entityType?: T;
}

export interface LocalDBSetsOptionsDatabases {
  [key: string]: LocalDBDefinition<any>
}

export interface LocalDBSetOptions<DBs extends LocalDBSetsOptionsDatabases> {
  path: string
  databases: DBs
}

export class LocalDB<DBs extends LocalDBSetsOptionsDatabases> {
  protected databases: {
    [K in keyof DBs]: DBs[K] extends LocalDBDefinition<infer T> ? LocalDBRepository<T, any> : never
  } = {} as any

  constructor(readonly options: LocalDBSetOptions<DBs>) {
    const keys = Object.keys(this.options.databases) as Array<keyof DBs>
    keys.forEach((dbName) => {
      const def = this.options.databases[dbName]
      ;(this.databases as any)[dbName] = new LocalDBRepository(this.options.path + '/' + String(dbName), def.options)
    })
  }

  public getDatabase<Name extends keyof DBs>(
    name: Name
  ): DBs[Name] extends LocalDBDefinition<infer T> ? LocalDBRepository<T, any> : never {
    return this.databases[name]
  }
}

export function defineDb<T extends LocalDBEntity>(options: LocalDBOptions<any>): LocalDBDefinition<T> {
  return new LocalDBDefinition(options)
}

/*
const t = new LocalDBSet({
  path: 'test',
  databases: {
    test: defineDb<{ name: string; test?: number }>({
      indexes: {
        name: {
          path: 'name',
        },
      },
    }),
  },
})

const data = await t.getDatabase('test').getOne('dfw')
*/
