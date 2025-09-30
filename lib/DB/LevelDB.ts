import { IteratorOptions, Level } from "level";
import * as cluster from 'cluster'
import { IpcMethodHandler } from "@david.uhlir/ipc-method";

export class LevelDB<K, T> {
  private db: Level<K, T>

  constructor(readonly unique: string, readonly dbPath: string) {
    this.db = new Level(dbPath, { valueEncoding: 'json' })
  }

  async open() {
    await this.db.open()
  }

  async close() {
    await this.db.close()
  }

  async get(key: K) {
    return this.db.get(key)
  }

  async put(key: K, value: T) {
    return this.db.put(key, value)
  }

  async del(key: K) {
    return this.db.del(key)
  }

  async getMany(keys: K[]) {
    return this.db.getMany(keys)
  }

  async clear() {
    return this.db.clear()
  }

  async query(itteratorOptions: IteratorOptions<any, T>, customOptions: { eq?: any, ne?: any } = {}): Promise<T[]> {
    const results: any[] = []
      for await (const [key, value] of this.db.iterator(itteratorOptions)) {
      if (customOptions.eq && key !== customOptions.eq) {
        continue
      }
      if (customOptions.ne && key === customOptions.ne) {
        continue
      }
      results.push(value)
    }
    return results
  }

  async getAll(): Promise<[K, T][]> {
    const items: [K, T][] = []
    const iterator = this.db.iterator()
    while (true) {
      const result = await iterator.next()
      if (result === undefined) break
      items.push(result)
    }
    await iterator.close()
    return items
  }
}

export class LevelDBCluster {
  static dbs: Record<string, any> = {}
  public static getInstance(unique: string, dbPath: string): LevelDB<any, any> {
    if (!cluster.default.isWorker) {
      LevelDBCluster.dbs[unique] = new LevelDB(unique, dbPath)
      new IpcMethodHandler(['__local-db-' + unique], LevelDBCluster.dbs[unique], { messageSizeLimit: 1024 })
    } else {
      const handler = new IpcMethodHandler(['__local-db-' + unique], { messageSizeLimit: 1024 })
      LevelDBCluster.dbs[unique] = handler.as<LevelDB<any, any>>()
    }
    return LevelDBCluster.dbs[unique]
  }
}
