import { expect } from 'chai'
import { LocalDB } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-indexing-conflicts')

describe('Indexing Conflict Tests', () => {
  let db: LocalDB<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new LocalDB(TEST_DB_PATH, {
      indexes: {
        status: {
          path: 'status',
        },
        counter: {
          path: 'counter',
        },
      },
    })
  })

  afterEach(async () => {
    // Close database
    await db.close()

    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }
  })

  describe('operations during remapIndex', () => {
    it('should handle deletes during remap', async function() {
      this.timeout(10000)

      // Insert 50 documents
      const ids: any[] = []
      for (let i = 0; i < 50; i++) {
        const id = await db.insert({ counter: i, status: 'active' })
        ids.push(id)
      }

      // Start remap and immediately delete some docs
      const operations: any[] = []
      operations.push(db.remapIndex())

      // Give remap a moment to start
      await new Promise(resolve => setTimeout(resolve, 5))

      // Delete 20 docs while remapping
      for (let i = 0; i < 20; i++) {
        operations.push(db.delete(ids[i]).catch(() => {}))
      }

      await Promise.all(operations)

      // Verify index consistency
      const indexed = await db.getIndex('status').get('active')

      // All indexed docs should actually exist
      for (const doc of indexed) {
        const exists = await db.exists(doc.$id)
        expect(exists).to.be.true
      }

      // Should have 30 docs (50 - 20 deleted)
      expect(indexed).to.have.lengthOf(30)
    })

    it('should handle edits during remap', async function() {
      this.timeout(10000)

      // Insert 50 documents
      const ids: any[] = []
      for (let i = 0; i < 50; i++) {
        const id = await db.insert({ counter: i, status: 'old' })
        ids.push(id)
      }

      // Start remap and immediately edit some docs
      const operations: any[] = []
      operations.push(db.remapIndex())

      await new Promise(resolve => setTimeout(resolve, 5))

      // Edit 20 docs to change status while remapping
      for (let i = 0; i < 20; i++) {
        operations.push(db.edit(ids[i], { status: 'new' }).catch(() => {}))
      }

      await Promise.all(operations)

      // Verify each doc is indexed under exactly one status
      const oldStatus = await db.getIndex('status').get('old')
      const newStatus = await db.getIndex('status').get('new')

      expect(oldStatus.length + newStatus.length).to.equal(50)

      // Check no duplicates
      const allIds = new Set([
        ...oldStatus.map(d => d.$id),
        ...newStatus.map(d => d.$id),
      ])
      expect(allIds.size).to.equal(50)
    })

    it('should handle inserts during remap', async function() {
      this.timeout(10000)

      // Insert initial 30 documents
      for (let i = 0; i < 30; i++) {
        await db.insert({ counter: i, status: 'initial' })
      }

      // Start remap and immediately insert more docs
      const operations: any[] = []
      operations.push(db.remapIndex())

      await new Promise(resolve => setTimeout(resolve, 5))

      // Insert 20 more docs while remapping
      for (let i = 30; i < 50; i++) {
        operations.push(db.insert({ counter: i, status: 'new' }))
      }

      await Promise.all(operations)

      // All docs should be properly indexed
      const initial = await db.getIndex('status').get('initial')
      const newDocs = await db.getIndex('status').get('new')

      expect(initial.length).to.equal(30)
      expect(newDocs.length).to.equal(20)
    })

    it('should handle mixed operations during remap', async function() {
      this.timeout(10000)

      // Insert initial documents
      const ids: any[] = []
      for (let i = 0; i < 40; i++) {
        const id = await db.insert({ counter: i, status: 'active' })
        ids.push(id)
      }

      // Start remap
      const operations: any[] = []
      operations.push(db.remapIndex())

      await new Promise(resolve => setTimeout(resolve, 5))

      // Mix of operations while remapping
      // Insert 10 new
      for (let i = 40; i < 50; i++) {
        operations.push(db.insert({ counter: i, status: 'new' }))
      }

      // Edit 10 existing
      for (let i = 0; i < 10; i++) {
        operations.push(db.edit(ids[i], { status: 'edited' }).catch(() => {}))
      }

      // Delete 10 existing
      for (let i = 10; i < 20; i++) {
        operations.push(db.delete(ids[i]).catch(() => {}))
      }

      await Promise.all(operations)

      // Verify index consistency
      const active = await db.getIndex('status').get('active')
      const edited = await db.getIndex('status').get('edited')
      const newDocs = await db.getIndex('status').get('new')

      const total = active.length + edited.length + newDocs.length

      // Should be 40 docs (40 initial - 10 deleted + 10 new)
      expect(total).to.equal(40)

      // No document should be indexed twice
      const allIds = new Set([
        ...active.map(d => d.$id),
        ...edited.map(d => d.$id),
        ...newDocs.map(d => d.$id),
      ])
      expect(allIds.size).to.equal(total)
    })

    it('should handle query during remap', async function() {
      this.timeout(10000)

      // Insert documents
      for (let i = 0; i < 50; i++) {
        await db.insert({ counter: i, status: 'active' })
      }

      // Start remap
      const operations: any[] = []
      operations.push(db.remapIndex())

      await new Promise(resolve => setTimeout(resolve, 5))

      // Query multiple times during remap
      for (let i = 0; i < 10; i++) {
        operations.push(db.getIndex('status').get('active'))
      }

      const results = await Promise.all(operations)

      // All queries should succeed (first one is remap result)
      const queryResults = results.slice(1) as any[]

      for (const result of queryResults) {
        expect(result).to.be.an('array')
        // Should return something close to 50
        expect(result.length).to.be.at.least(40)
      }
    })
  })

  describe('rapid index changes', () => {
    it('should handle document being edited while index is being updated', async function() {
      this.timeout(10000)

      const id = await db.insert({ counter: 0, status: 'initial' })

      // Rapidly change status many times
      const operations: any[] = []
      for (let i = 0; i < 20; i++) {
        operations.push(
          db.edit(id, { status: `status-${i}` })
        )
      }

      await Promise.all(operations)

      // Document should exist
      const doc: any = await db.getOne(id)
      expect(doc).to.not.be.null

      // Should be indexed under exactly one status
      let foundCount = 0
      for (let i = 0; i < 20; i++) {
        const results = await db.getIndex('status').get(`status-${i}`)
        foundCount += results.filter(d => d.$id === id).length
      }

      const initialResults = await db.getIndex('status').get('initial')
      foundCount += initialResults.filter(d => d.$id === id).length

      expect(foundCount).to.equal(1)
    })

    it('should handle document deleted while being indexed', async function() {
      this.timeout(10000)

      const ids: any[] = []
      for (let i = 0; i < 20; i++) {
        const id = await db.insert({ counter: i, status: 'temp' })
        ids.push(id)
      }

      // Immediately delete half of them
      const operations: any[] = []
      for (let i = 0; i < 10; i++) {
        operations.push(db.delete(ids[i]))
      }

      await Promise.all(operations)

      // Index should only contain remaining docs
      const indexed = await db.getIndex('status').get('temp')
      expect(indexed).to.have.lengthOf(10)

      // All indexed docs should exist
      for (const doc of indexed) {
        const exists = await db.exists(doc.$id)
        expect(exists).to.be.true
      }
    })

    it('should handle multiple concurrent remaps', async function() {
      this.timeout(10000)

      // Insert documents
      for (let i = 0; i < 30; i++) {
        await db.insert({ counter: i, status: 'active' })
      }

      // Try to remap multiple times concurrently
      // The mutex should serialize these
      const operations: any[] = []
      for (let i = 0; i < 3; i++) {
        operations.push(db.remapIndex())
      }

      await Promise.all(operations)

      // Index should be consistent
      const indexed = await db.getIndex('status').get('active')
      expect(indexed).to.have.lengthOf(30)

      // No duplicates
      const uniqueIds = new Set(indexed.map(d => d.$id))
      expect(uniqueIds.size).to.equal(30)
    })
  })

  describe('index corruption scenarios', () => {
    it('should recover from index pointing to deleted document', async function() {
      this.timeout(10000)

      // Insert documents
      const ids: any[] = []
      for (let i = 0; i < 20; i++) {
        const id = await db.insert({ counter: i, status: 'active' })
        ids.push(id)
      }

      // Delete half rapidly
      const deleteOps: any[] = []
      for (let i = 0; i < 10; i++) {
        deleteOps.push(db.delete(ids[i]))
      }
      await Promise.all(deleteOps)

      // Query should only return existing docs
      const results = await db.getIndex('status').get('active')

      for (const doc of results) {
        const exists = await db.exists(doc.$id)
        expect(exists).to.be.true
      }
    })

    it('should handle edit changing indexed field while query is running', async function() {
      this.timeout(10000)

      const id = await db.insert({ counter: 0, status: 'pending' })

      const operations: any[] = []

      // Start a query
      operations.push(db.getIndex('status').get('pending'))

      // Immediately edit to change status
      operations.push(db.edit(id, { status: 'active' }))

      // Start another query for new status
      operations.push(db.getIndex('status').get('active'))

      const [pendingResults, , activeResults] = await Promise.all(operations)

      // Document should be indexed under exactly one status
      const inPending = (pendingResults as any[]).some(d => d.$id === id)
      const inActive = (activeResults as any[]).some(d => d.$id === id)

      expect(inPending || inActive).to.be.true
      expect(inPending && inActive).to.be.false // Not in both
    })

    it('should maintain consistency when same document edited by multiple operations', async function() {
      this.timeout(10000)

      const id = await db.insert({ counter: 0, status: 'initial' })

      // 10 operations trying to change status simultaneously
      const operations: any[] = []
      const statuses = ['a', 'b', 'c', 'd', 'e']

      for (let i = 0; i < 10; i++) {
        operations.push(
          db.edit(id, { status: statuses[i % statuses.length] })
        )
      }

      await Promise.all(operations)

      // Document should be indexed under exactly one status
      let foundCount = 0
      for (const status of [...statuses, 'initial']) {
        const results = await db.getIndex('status').get(status)
        foundCount += results.filter(d => d.$id === id).length
      }

      expect(foundCount).to.equal(1)

      // The actual document should exist and be consistent
      const doc: any = await db.getOne(id)
      expect(doc).to.not.be.null

      // Verify doc is indexed under its current status
      const currentStatusResults = await db.getIndex('status').get(doc.status)
      expect(currentStatusResults.some(d => d.$id === id)).to.be.true
    })
  })

  describe('extreme edge cases', () => {
    it('should handle remap while actively deleting all documents', async function() {
      this.timeout(15000)

      const ids: any[] = []
      for (let i = 0; i < 40; i++) {
        const id = await db.insert({ counter: i, status: 'temp' })
        ids.push(id)
      }

      const operations: any[] = []

      // Start remap
      operations.push(db.remapIndex())

      await new Promise(resolve => setTimeout(resolve, 5))

      // Delete all documents
      for (const id of ids) {
        operations.push(db.delete(id).catch(() => {}))
      }

      await Promise.all(operations)

      // Index should be empty
      const indexed = await db.getIndex('status').get('temp')
      expect(indexed).to.have.lengthOf(0)
    })

    it('should handle rapid create-delete cycles during remap', async function() {
      this.timeout(15000)

      // Initial documents
      for (let i = 0; i < 20; i++) {
        await db.insert({ counter: i, status: 'initial' })
      }

      const operations: any[] = []

      // Start remap
      operations.push(db.remapIndex())

      await new Promise(resolve => setTimeout(resolve, 5))

      // Create and delete documents rapidly
      for (let i = 0; i < 10; i++) {
        const insertPromise = db.insert({ counter: i + 100, status: 'temp' })
        operations.push(insertPromise)

        insertPromise.then(id => {
          operations.push(db.delete(id).catch(() => {}))
        })
      }

      await Promise.all(operations)

      // Should have at least the initial docs
      const initial = await db.getIndex('status').get('initial')
      expect(initial.length).to.be.at.least(15)

      // Temp docs should be minimal (most deleted)
      const temp = await db.getIndex('status').get('temp')
      expect(temp.length).to.be.at.most(5)
    })
  })
})