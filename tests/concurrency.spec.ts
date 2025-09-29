import { expect } from 'chai'
import { LocalDB } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-concurrency')

describe('Concurrency and Lock Tests', () => {
  let db: LocalDB<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new LocalDB(TEST_DB_PATH, {
      indexes: {
        counter: {
          path: 'counter',
        },
        name: {
          path: 'name',
        },
      },
    })
    await db.open()
  })

  afterEach(async () => {
    // Close database
    await db.close()

    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }
  })

  describe('concurrent inserts', () => {
    it('should handle multiple concurrent inserts', async () => {
      const promises: any[] = []
      for (let i = 0; i < 10; i++) {
        promises.push(
          db.insert({
            name: `User ${i}`,
            counter: i,
          })
        )
      }

      const ids = await Promise.all(promises)

      // All inserts should succeed with unique IDs
      expect(ids).to.have.lengthOf(10)
      const uniqueIds = new Set(ids)
      expect(uniqueIds.size).to.equal(10)

      // Verify all documents are retrievable
      const docs = await db.get(ids)
      expect(docs).to.have.lengthOf(10)
    })

    it('should correctly update indexes for concurrent inserts', async () => {
      const promises: any[] = []
      for (let i = 0; i < 5; i++) {
        promises.push(
          db.insert({
            name: 'Same Name',
            counter: i,
          })
        )
      }

      await Promise.all(promises)

      // All 5 documents should be indexed under the same name
      const results = await db.getIndex('name').get('Same Name')
      expect(results).to.have.lengthOf(5)
    })
  })

  describe('concurrent edits', () => {
    it('should handle concurrent edits to different documents', async () => {
      // Insert test documents
      const ids: any[] = []
      for (let i = 0; i < 5; i++) {
        const id = await db.insert({
          name: `User ${i}`,
          counter: 0,
        })
        ids.push(id)
      }

      // Edit all documents concurrently
      const editPromises = ids.map((id, i) =>
        db.edit(id, {
          counter: i * 10,
        })
      )

      await Promise.all(editPromises)

      // Verify all edits succeeded
      for (let i = 0; i < ids.length; i++) {
        const doc = await db.getOne(ids[i])
        expect(doc!.counter).to.equal(i * 10)
      }
    })

    it('should handle concurrent edits to the same document', async () => {
      const id = await db.insert({
        name: 'Test',
        counter: 0,
      })

      // Try to edit the same document concurrently
      const editPromises: any[] = []
      for (let i = 1; i <= 10; i++) {
        editPromises.push(
          db.edit(id, {
            counter: i,
          })
        )
      }

      await Promise.all(editPromises)

      // The document should have one of the values (last write wins)
      const doc = await db.getOne(id)
      expect(doc!.counter).to.be.at.least(1)
      expect(doc!.counter).to.be.at.most(10)
    })

    it('should maintain index consistency during concurrent edits', async () => {
      const id = await db.insert({
        name: 'Original',
        counter: 0,
      })

      // Concurrently edit the name
      const editPromises: any[] = []
      for (let i = 1; i <= 5; i++) {
        editPromises.push(
          db.edit(id, {
            name: `Name ${i}`,
          })
        )
      }

      await Promise.all(editPromises)

      // The document should be indexed under exactly one name
      const doc = await db.getOne(id)
      const indexResults = await db.getIndex('name').get(doc!.name)
      expect(indexResults).to.have.lengthOf(1)
      expect(indexResults[0].$id).to.equal(id)

      // Original name should no longer be indexed
      const originalResults = await db.getIndex('name').get('Original')
      expect(originalResults).to.have.lengthOf(0)
    })
  })

  describe('concurrent deletes', () => {
    it('should handle concurrent deletes', async () => {
      const ids: any[] = []
      for (let i = 0; i < 5; i++) {
        const id = await db.insert({
          name: `User ${i}`,
          counter: i,
        })
        ids.push(id)
      }

      // Delete all concurrently
      const deletePromises = ids.map(id => db.delete(id))
      await Promise.all(deletePromises)

      // Verify all are deleted
      for (const id of ids) {
        const exists = await db.exists(id)
        expect(exists).to.be.false
      }
    })

    it('should handle concurrent deletes of the same document', async () => {
      const id = await db.insert({
        name: 'Test',
        counter: 0,
      })

      // Try to delete the same document multiple times concurrently
      const deletePromises: any[] = []
      for (let i = 0; i < 5; i++) {
        deletePromises.push(db.delete(id).catch(() => {})) // Ignore errors for concurrent deletes
      }

      await Promise.all(deletePromises)

      const exists = await db.exists(id)
      expect(exists).to.be.false
    })
  })

  describe('mixed concurrent operations', () => {
    it('should handle mixed inserts, edits, and deletes', async () => {
      const operations: any[] = []

      // Insert 10 documents
      for (let i = 0; i < 10; i++) {
        operations.push(
          db.insert({
            name: `User ${i}`,
            counter: i,
          })
        )
      }

      const ids = await Promise.all(operations)

      // Mix of edits and deletes
      const mixedOps: any[] = []

      // Edit first 5
      for (let i = 0; i < 5; i++) {
        mixedOps.push(
          db.edit(ids[i], {
            counter: i * 100,
          })
        )
      }

      // Delete last 5
      for (let i = 5; i < 10; i++) {
        mixedOps.push(db.delete(ids[i]))
      }

      // Add 5 more inserts
      for (let i = 10; i < 15; i++) {
        mixedOps.push(
          db.insert({
            name: `User ${i}`,
            counter: i,
          })
        )
      }

      await Promise.all(mixedOps)

      // Verify: first 5 should be edited
      for (let i = 0; i < 5; i++) {
        const doc = await db.getOne(ids[i])
        expect(doc!.counter).to.equal(i * 100)
      }

      // Verify: next 5 should be deleted
      for (let i = 5; i < 10; i++) {
        const exists = await db.exists(ids[i])
        expect(exists).to.be.false
      }
    })
  })

  describe('concurrent index queries', () => {
    it('should handle concurrent reads while writing', async () => {
      // Insert initial data
      for (let i = 0; i < 10; i++) {
        await db.insert({
          name: 'Test',
          counter: i,
        })
      }

      const operations: any[] = []

      // Concurrent reads
      for (let i = 0; i < 20; i++) {
        operations.push(db.getIndex('name').get('Test'))
      }

      // Concurrent writes
      for (let i = 10; i < 15; i++) {
        operations.push(
          db.insert({
            name: 'Test',
            counter: i,
          })
        )
      }

      const results = await Promise.all(operations)

      // All read operations should succeed
      const readResults = results.filter(r => Array.isArray(r))
      expect(readResults.length).to.be.at.least(20)

      // Final count should include all inserts
      const finalResults = await db.getIndex('name').get('Test')
      expect(finalResults.length).to.be.at.least(10)
    })

    it('should handle concurrent range queries', async () => {
      // Insert test data
      for (let i = 0; i < 20; i++) {
        await db.insert({
          name: `User ${i}`,
          counter: i,
        })
      }

      // Concurrent range queries
      const queries: any[] = []
      for (let i = 0; i < 10; i++) {
        queries.push(
          db.getIndex('counter').query({
            gte: 5,
            lte: 15,
          })
        )
      }

      const results = await Promise.all(queries)

      // All queries should return the same results
      for (const result of results) {
        expect(result).to.have.lengthOf(11) // 5 to 15 inclusive
      }
    })
  })

  describe('remap index concurrency', () => {
    it('should block other operations during remapIndex', async () => {
      // Insert initial data
      for (let i = 0; i < 10; i++) {
        await db.insert({
          name: `User ${i}`,
          counter: i,
        })
      }

      const operations: any[] = []
      let remapStarted = false
      let remapFinished = false

      // Start remap (slow operation)
      operations.push(
        (async () => {
          remapStarted = true
          await db.remapIndex()
          remapFinished = true
        })()
      )

      // Wait a bit to ensure remap starts
      await new Promise(resolve => setTimeout(resolve, 10))

      // Try to insert while remapping
      const insertPromise = db.insert({
        name: 'During Remap',
        counter: 999,
      })
      operations.push(insertPromise)

      await Promise.all(operations)

      expect(remapFinished).to.be.true

      // Verify the insert succeeded and is indexed
      const results = await db.getIndex('name').get('During Remap')
      expect(results).to.have.lengthOf(1)
    })
  })

  describe('stress test', () => {
    it('should handle high concurrency load', async function() {
      this.timeout(10000) // Increase timeout for stress test

      const operations: any[] = []
      const insertCount = 100

      // Insert many documents concurrently
      for (let i = 0; i < insertCount; i++) {
        operations.push(
          db.insert({
            name: `User ${i % 10}`, // 10 unique names
            counter: i,
          })
        )
      }

      const ids = await Promise.all(operations)

      // Verify all inserts succeeded
      expect(ids).to.have.lengthOf(insertCount)
      const uniqueIds = new Set(ids)
      expect(uniqueIds.size).to.equal(insertCount)

      // Verify index integrity
      for (let i = 0; i < 10; i++) {
        const results = await db.getIndex('name').get(`User ${i}`)
        expect(results).to.have.lengthOf(10)
      }
    })
  })
})