import { JsonDBEntity } from './interfaces'
import { JsonDBRepository, JsonDBOptions } from './components/JsonDBRepository'

export class JsonDBDefinition<T extends JsonDBEntity> {
  constructor(readonly options: JsonDBOptions<any>) {}
  declare __entityType?: T;
}

export interface JsonDBSetsOptionsDatabases {
  [key: string]: JsonDBDefinition<any>
}

export interface JsonDBSetOptions<DBs extends JsonDBSetsOptionsDatabases> {
  path: string
  databases: DBs
}

export class JsonDB<DBs extends JsonDBSetsOptionsDatabases> {
  protected databases: {
    [K in keyof DBs]: DBs[K] extends JsonDBDefinition<infer T> ? JsonDBRepository<T, any> : never
  } = {} as any

  constructor(readonly options: JsonDBSetOptions<DBs>) {
    const keys = Object.keys(this.options.databases) as Array<keyof DBs>
    keys.forEach((dbName) => {
      const def = this.options.databases[dbName]
      ;(this.databases as any)[dbName] = new JsonDBRepository(this.options.path + '/' + String(dbName), def.options)
    })
  }

  public getDatabase<Name extends keyof DBs>(
    name: Name
  ): DBs[Name] extends JsonDBDefinition<infer T> ? JsonDBRepository<T, any> : never {
    return this.databases[name]
  }
}

export function defineDb<T extends JsonDBEntity>(options: JsonDBOptions<any>): JsonDBDefinition<T> {
  return new JsonDBDefinition(options)
}

/*
const t = new JsonDBSet({
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
