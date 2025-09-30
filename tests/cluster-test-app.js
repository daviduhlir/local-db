const { LocalDB } = require('../dist')
const path = require('path')
const fs = require('fs')
const cluster = require('cluster')

const TEST_DB_PATH = path.join(__dirname, '.test-db-cluster')
const NUM_WORKERS = 3
const INSERTS_PER_WORKER = 5

async function master() {
  console.log('[MASTER] Starting cluster test')

  // Clean up
  if (fs.existsSync(TEST_DB_PATH)) {
    fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
  }

  const workers = []
  const insertedIds = []

  // Fork workers
  for (let i = 0; i < NUM_WORKERS; i++) {
    const worker = cluster.fork({ WORKER_ID: i.toString() })
    workers.push(worker)
    console.log(`[MASTER] Forked worker ${i}`)
  }

  // Wait for all workers to be ready
  await Promise.all(
    workers.map(
      (worker, i) =>
        new Promise(resolve => {
          const handler = (msg) => {
            if (msg.type === 'ready') {
              console.log(`[MASTER] Worker ${i} ready`)
              worker.off('message', handler)
              resolve()
            }
          }
          worker.on('message', handler)
        })
    )
  )

  console.log('[MASTER] All workers ready, starting inserts')

  // Send insert commands to all workers
  const insertPromises = workers.map((worker, workerIndex) => {
    return new Promise((resolve, reject) => {
      let insertCount = 0

      const handler = (msg) => {
        if (msg.type === 'inserted') {
          insertedIds.push(msg.id)
          console.log(`[MASTER] Worker ${workerIndex} inserted: ${msg.id} (${insertCount + 1}/${INSERTS_PER_WORKER})`)
          insertCount++

          if (insertCount < INSERTS_PER_WORKER) {
            worker.send({
              type: 'insert',
              data: {
                counter: insertCount,
                workerId: workerIndex,
                message: `Worker ${workerIndex} insert ${insertCount}`,
              },
            })
          } else {
            worker.off('message', handler)
            resolve()
          }
        } else if (msg.type === 'error') {
          console.error(`[MASTER] Worker ${workerIndex} error: ${msg.error}`)
          worker.off('message', handler)
          reject(new Error(msg.error))
        }
      }

      worker.on('message', handler)

      // Send first insert
      worker.send({
        type: 'insert',
        data: {
          counter: 0,
          workerId: workerIndex,
          message: `Worker ${workerIndex} insert 0`,
        },
      })
    })
  })

  await Promise.all(insertPromises)

  console.log(`[MASTER] All inserts complete. Total: ${insertedIds.length}`)

  // Cleanup workers
  await Promise.all(
    workers.map(
      (worker, i) =>
        new Promise(resolve => {
          worker.send({ type: 'done' })
          worker.on('exit', () => {
            console.log(`[MASTER] Worker ${i} exited`)
            resolve()
          })
        })
    )
  )

  // Give time for cleanup
  await new Promise(resolve => setTimeout(resolve, 500))

  console.log('[MASTER] Verifying data in DB')

  // Verify data
  const db = new LocalDB(TEST_DB_PATH, {
    indexes: {
      counter: { path: 'counter' },
      workerId: { path: 'workerId' },
    },
  })

  await db.open()

  // Check unique IDs
  const uniqueIds = new Set(insertedIds)
  if (uniqueIds.size !== insertedIds.length) {
    console.error(`[MASTER] ERROR: Expected ${insertedIds.length} unique IDs, got ${uniqueIds.size}`)
    process.exit(1)
  }
  console.log(`[MASTER] ✓ All ${insertedIds.length} IDs are unique`)

  // Check each worker's data
  for (let i = 0; i < NUM_WORKERS; i++) {
    const results = await db.getIndex('workerId').get(i)
    if (results.length !== INSERTS_PER_WORKER) {
      console.error(`[MASTER] ERROR: Worker ${i} expected ${INSERTS_PER_WORKER} documents, got ${results.length}`)
      process.exit(1)
    }
    console.log(`[MASTER] ✓ Worker ${i} has ${results.length} documents`)
  }

  await db.close()

  // Cleanup
  if (fs.existsSync(TEST_DB_PATH)) {
    fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
  }

  console.log('[MASTER] ✓ All tests passed!')
  process.exit(0)
}

async function worker() {
  const workerId = process.env.WORKER_ID || '?'
  console.log(`[WORKER ${workerId}] Starting`)

  const db = new LocalDB(TEST_DB_PATH, {
    indexes: {
      counter: { path: 'counter' },
      workerId: { path: 'workerId' },
    },
  })

  await db.open()
  console.log(`[WORKER ${workerId}] DB opened`)

  process.send({ type: 'ready' })

  process.on('message', async (message) => {
    try {
      switch (message.type) {
        case 'insert':
          const id = await db.insert(message.data)
          process.send({ type: 'inserted', id })
          break

        case 'done':
          await db.close()
          console.log(`[WORKER ${workerId}] Exiting`)
          process.exit(0)
          break
      }
    } catch (error) {
      console.error(`[WORKER ${workerId}] Error:`, error.message)
      process.send({ type: 'error', error: error.message })
    }
  })
}

// Main
if (!cluster.isWorker) {
  master().catch(err => {
    console.error('[MASTER] Fatal error:', err)
    process.exit(1)
  })
} else {
  setTimeout(() => {
    worker().catch(err => {
      console.error('[WORKER] Fatal error:', err)
      process.exit(1)
    })
  }, 200)
}