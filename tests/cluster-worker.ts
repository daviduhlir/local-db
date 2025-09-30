import { LocalDB } from '../lib'
import * as path from 'path'

const TEST_DB_PATH = path.join(__dirname, '.test-db-cluster')

interface WorkerMessage {
  type: 'insert' | 'edit' | 'delete' | 'query' | 'done'
  id?: string
  data?: any
  count?: number
}

const db = new LocalDB(TEST_DB_PATH, {
  indexes: {
    counter: {
      path: 'counter',
    },
    workerId: {
      path: 'workerId',
    },
  },
})

async function handleMessage(message: WorkerMessage) {
  try {
    switch (message.type) {
      case 'insert':
        const id = await db.insert(message.data)
        process.send!({ type: 'inserted', id })
        break

      case 'edit':
        await db.edit(message.id!, message.data)
        process.send!({ type: 'edited', id: message.id })
        break

      case 'delete':
        await db.delete(message.id!)
        process.send!({ type: 'deleted', id: message.id })
        break

      case 'query':
        const results = await db.getIndex('workerId').get(message.data.workerId)
        process.send!({ type: 'query-result', count: results.length })
        break

      case 'done':
        await db.close()
        process.exit(0)
        break
    }
  } catch (error: any) {
    process.send!({ type: 'error', error: error.message })
  }
}

async function init() {
  await db.open()
  process.send!({ type: 'ready' })

  process.on('message', (message: WorkerMessage) => {
    handleMessage(message)
  })
}

init()