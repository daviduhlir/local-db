import { expect } from 'chai'
import { LocalDBRepository } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-race')

describe('Race Condition Tests', () => {
  let db: LocalDBRepository<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new LocalDBRepository(TEST_DB_PATH, {
      indexes: {
        counter: {
          path: 'counter',
        },
        status: {
          path: 'status',
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

  describe('basic race conditions', () => {
    it('should handle 10 rapid edits on same document', async function() {
      this.timeout(5000)

      const id = await db.insert({ counter: 0 })

      const editPromises: any[] = []
      for (let i = 1; i <= 10; i++) {
        editPromises.push(db.edit(id, { counter: i }))
      }

      await Promise.all(editPromises)

      const doc: any = await db.getOne(id)
      expect(doc).to.not.be.null
      expect(doc.counter).to.be.at.least(1)
      expect(doc.counter).to.be.at.most(10)
    })

    it('should handle edit + delete race', async function() {
      this.timeout(5000)

      const id = await db.insert({ counter: 0 })

      const operations: any[] = []

      // 5 edits
      for (let i = 1; i <= 5; i++) {
        operations.push(db.edit(id, { counter: i }).catch(() => {}))
      }

      // 1 delete
      operations.push(db.delete(id).catch(() => {}))

      await Promise.all(operations)

      // Should end up deleted
      const exists = await db.exists(id)
      expect(exists).to.be.false
    })

    it('should handle concurrent inserts with same index value', async function() {
      this.timeout(5000)

      const promises: any[] = []
      for (let i = 0; i < 10; i++) {
        promises.push(db.insert({ counter: 5, status: 'active' }))
      }

      const ids = await Promise.all(promises)
      expect(ids).to.have.lengthOf(10)

      const results = await db.getIndex('counter').get(5)
      expect(results).to.have.lengthOf(10)
    })
  })

  describe('index consistency', () => {
    it('should maintain index when rapidly changing indexed field', async function() {
      this.timeout(5000)

      const id = await db.insert({ status: 'pending' })

      const statuses = ['active', 'inactive', 'deleted']
      const operations: any[] = []

      for (let i = 0; i < 10; i++) {
        operations.push(db.edit(id, { status: statuses[i % statuses.length] }))
      }

      await Promise.all(operations)

      const doc: any = await db.getOne(id)
      expect(doc).to.not.be.null

      // Should be indexed under exactly one status
      let found = 0
      for (const status of ['pending', ...statuses]) {
        const results = await db.getIndex('status').get(status)
        found += results.filter(r => r.$id === id).length
      }

      expect(found).to.equal(1)
    })

    it('should handle concurrent edits changing different fields', async function() {
      this.timeout(5000)

      const ids: any[] = []
      for (let i = 0; i < 5; i++) {
        const id = await db.insert({ counter: 0, status: 'old' })
        ids.push(id)
      }

      const editPromises = ids.map((id, i) => db.edit(id, { counter: i }))

      await Promise.all(editPromises)

      for (let i = 0; i < ids.length; i++) {
        const doc: any = await db.getOne(ids[i])
        expect(doc.counter).to.equal(i)
      }
    })

    it('should not have orphaned index entries after delete', async function() {
      this.timeout(5000)

      const ids: any[] = []
      for (let i = 0; i < 10; i++) {
        const id = await db.insert({ counter: i, status: 'temp' })
        ids.push(id)
      }

      // Delete half
      const deletePromises: any[] = []
      for (let i = 0; i < 5; i++) {
        deletePromises.push(db.delete(ids[i]))
      }

      await Promise.all(deletePromises)

      // Check index points only to existing docs
      const indexed = await db.getIndex('status').get('temp')
      expect(indexed).to.have.lengthOf(5)

      for (const doc of indexed) {
        const exists = await db.exists(doc.$id)
        expect(exists).to.be.true
      }
    })
  })

  describe('concurrent mixed operations', () => {
    it('should handle 50 mixed operations', async function() {
      this.timeout(10000)

      // Start with some docs
      const initialIds: any[] = []
      for (let i = 0; i < 10; i++) {
        const id = await db.insert({ counter: i, status: 'initial' })
        initialIds.push(id)
      }

      const operations: any[] = []

      // 20 more inserts
      for (let i = 10; i < 30; i++) {
        operations.push(db.insert({ counter: i, status: 'new' }))
      }

      // 10 edits
      for (let i = 0; i < 5; i++) {
        operations.push(db.edit(initialIds[i], { status: 'edited' }).catch(() => {}))
      }

      // 5 deletes
      for (let i = 5; i < 10; i++) {
        operations.push(db.delete(initialIds[i]).catch(() => {}))
      }

      // 15 queries
      for (let i = 0; i < 15; i++) {
        operations.push(db.getIndex('status').query({ ne: null }).catch(() => []))
      }

      await Promise.all(operations)

      // Count indexed docs
      const statuses = ['initial', 'new', 'edited']
      let totalIndexed = 0
      for (const status of statuses) {
        const results = await db.getIndex('status').get(status)
        totalIndexed += results.length
      }

      // Should be 25 docs (10 initial - 5 deleted + 20 new)
      expect(totalIndexed).to.equal(25)
    })

    it('should handle rapid insert-edit-delete cycles', async function() {
      this.timeout(10000)

      for (let i = 0; i < 10; i++) {
        const id = await db.insert({ counter: i, status: 'temp' })
        await db.edit(id, { counter: i * 2 })
        await db.delete(id)
      }

      // All should be deleted
      const remaining = await db.getIndex('status').get('temp')
      expect(remaining).to.have.lengthOf(0)
    })
  })

  describe('stress tests (smaller scale)', () => {
    it('should handle 50 concurrent inserts', async function() {
      this.timeout(10000)

      const promises: any[] = []
      for (let i = 0; i < 50; i++) {
        promises.push(
          db.insert({
            counter: i % 10,
            status: i % 2 === 0 ? 'active' : 'inactive',
          })
        )
      }

      const ids = await Promise.all(promises)
      expect(ids).to.have.lengthOf(50)

      // Verify index consistency
      const active = await db.getIndex('status').get('active')
      const inactive = await db.getIndex('status').get('inactive')

      expect(active).to.have.lengthOf(25)
      expect(inactive).to.have.lengthOf(25)
    })

    it('should handle 100 operations (insert/edit/delete/query)', async function() {
      this.timeout(15000)

      const operations: any[] = []
      const idPromises: any[] = []

      // 30 inserts
      for (let i = 0; i < 30; i++) {
        idPromises.push(db.insert({ counter: i, status: 'active' }))
      }

      operations.push(...idPromises)

      const ids = await Promise.all(idPromises)

      // 30 edits
      for (let i = 0; i < 30; i++) {
        const randomId = ids[Math.floor(Math.random() * ids.length)]
        operations.push(db.edit(randomId, { counter: Math.random() * 100 }).catch(() => {}))
      }

      // 20 deletes
      for (let i = 0; i < 20; i++) {
        operations.push(db.delete(ids[i]).catch(() => {}))
      }

      // 20 queries
      for (let i = 0; i < 20; i++) {
        operations.push(db.getIndex('status').get('active').catch(() => []))
      }

      await Promise.all(operations)

      // Should have ~10 docs remaining
      const remaining = await db.getIndex('status').get('active')
      expect(remaining.length).to.be.at.least(5)
      expect(remaining.length).to.be.at.most(30)
    })
  })
})