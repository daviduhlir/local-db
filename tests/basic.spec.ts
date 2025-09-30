import { expect } from 'chai'
import { LocalDBRepository } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db')

describe('Basic CRUD Tests', () => {
  let db: LocalDBRepository<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new LocalDBRepository(TEST_DB_PATH, {
      indexes: {
        name: {
          path: 'name',
        },
        age: {
          path: 'info.age',
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

  describe('insert', () => {
    it('should insert a document and return an id', async () => {
      const id = await db.insert({
        name: 'David',
        info: {
          age: 30,
        },
      })

      expect(id).to.be.a('string')
      expect(id).to.have.lengthOf(32)
    })

    it('should insert multiple documents with unique ids', async () => {
      const id1 = await db.insert({ name: 'David' })
      const id2 = await db.insert({ name: 'John' })

      expect(id1).to.not.equal(id2)
    })
  })

  describe('getOne', () => {
    it('should retrieve a document by id', async () => {
      const id = await db.insert({
        name: 'David',
        info: {
          age: 30,
        },
      })

      const doc = await db.getOne(id)

      expect(doc).to.not.be.null
      expect(doc!.$id).to.equal(id)
      expect(doc!.name).to.equal('David')
      expect(doc!.info.age).to.equal(30)
    })

    it('should return null for non-existent id', async () => {
      const doc = await db.getOne('non-existent-id')
      expect(doc).to.be.null
    })
  })

  describe('get', () => {
    it('should retrieve multiple documents by ids', async () => {
      const id1 = await db.insert({ name: 'David', info: { age: 30 } })
      const id2 = await db.insert({ name: 'John', info: { age: 40 } })
      const id3 = await db.insert({ name: 'Jane', info: { age: 50 } })

      const docs = await db.get([id1, id2, id3])

      expect(docs).to.have.lengthOf(3)
      expect(docs.map(d => d.name)).to.include.members(['David', 'John', 'Jane'])
    })

    it('should handle empty array', async () => {
      const docs = await db.get([])
      expect(docs).to.have.lengthOf(0)
    })
  })

  describe('exists', () => {
    it('should return true for existing document', async () => {
      const id = await db.insert({ name: 'David' })
      const exists = await db.exists(id)

      expect(exists).to.be.true
    })

    it('should return false for non-existent document', async () => {
      const exists = await db.exists('non-existent-id')
      expect(exists).to.be.false
    })
  })

  describe('edit', () => {
    it('should update a document', async () => {
      const id = await db.insert({
        name: 'David',
        info: {
          age: 30,
        },
      })

      await db.edit(id, {
        info: {
          age: 31,
        },
      })

      const doc = await db.getOne(id)
      expect(doc!.name).to.equal('David')
      expect(doc!.info.age).to.equal(31)
    })

    it('should throw error for non-existent document', async () => {
      try {
        await db.edit('non-existent-id', { name: 'Test' })
        expect.fail('Should have thrown error')
      } catch (e: any) {
        expect(e.message).to.include('Item not found')
      }
    })

    it('should partially update document', async () => {
      const id = await db.insert({
        name: 'David',
        email: 'david@example.com',
        info: {
          age: 30,
          city: 'Prague',
        },
      })

      await db.edit(id, {
        name: 'David Updated',
      })

      const doc = await db.getOne(id)
      expect(doc!.name).to.equal('David Updated')
      expect(doc!.email).to.equal('david@example.com')
      expect(doc!.info.age).to.equal(30)
    })
  })

  describe('delete', () => {
    it('should delete a document', async () => {
      const id = await db.insert({ name: 'David' })

      const existsBefore = await db.exists(id)
      expect(existsBefore).to.be.true

      await db.delete(id)

      const existsAfter = await db.exists(id)
      expect(existsAfter).to.be.false
    })

    it('should throw error when deleting non-existent document', async () => {
      try {
        await db.delete('non-existent-id')
        expect.fail('Should have thrown error')
      } catch (e: any) {
        expect(e.message).to.include('Item not found')
      }
    })
  })
})