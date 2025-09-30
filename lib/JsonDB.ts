import { JsonDBEntity } from './interfaces'
import { JsonDBRepository, JsonDBOptions } from './components/JsonDBRepository'

export class JsonDBRepositoryDefinition<T extends JsonDBEntity> {
  constructor(readonly options: JsonDBOptions<any>) {}
  declare __entityType?: T;
}

export interface JsonDBSetsOptionsDatabases {
  [key: string]: JsonDBRepositoryDefinition<any>
}

export interface JsonDBSetOptions<DBs extends JsonDBSetsOptionsDatabases> {
  path: string
  databases: DBs
}

export class JsonDB<DBs extends JsonDBSetsOptionsDatabases> {
  protected databases: {
    [K in keyof DBs]: DBs[K] extends JsonDBRepositoryDefinition<infer T> ? JsonDBRepository<T, any> : never
  } = {} as any

  constructor(readonly options: JsonDBSetOptions<DBs>) {
    const keys = Object.keys(this.options.databases) as Array<keyof DBs>
    if (keys.some(k => typeof k !== 'string' || k.match(/[^a-zA-Z0-9]/))) {
      throw new Error('Database name can contain only letters and numbers')
    }
    keys.forEach((dbName) => {
      const def = this.options.databases[dbName]
      ;(this.databases as any)[dbName] = new JsonDBRepository(this.options.path + '/' + String(dbName), def.options)
    })
  }

  public getRepository<Name extends keyof DBs>(
    name: Name
  ): DBs[Name] extends JsonDBRepositoryDefinition<infer T> ? JsonDBRepository<T, any> : never {
    return this.databases[name]
  }
}

export function repository<T extends JsonDBEntity>(options: JsonDBOptions<any>): JsonDBRepositoryDefinition<T> {
  return new JsonDBRepositoryDefinition(options)
}

/*
const t = new JsonDBSet({
  path: 'test',
  databases: {
    test: repository<{ name: string; test?: number }>({
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
