import { expect } from 'chai'
import { LocalDB } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-indexes')

describe('Index Query Tests', () => {
  let db: LocalDB<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new LocalDB(TEST_DB_PATH, {
      indexes: {
        name: {
          path: 'name',
        },
        age: {
          path: 'info.age',
        },
        email: {
          path: 'email',
        },
        active: {
          path: 'active',
        },
        createdAt: {
          path: 'createdAt',
        },
      },
    })

    // Insert test data
    await db.insert({
      name: 'David',
      email: 'david@example.com',
      info: { age: 30 },
      active: true,
      createdAt: new Date('2024-01-15'),
    })
    await db.insert({
      name: 'John',
      email: 'john@example.com',
      info: { age: 40 },
      active: true,
      createdAt: new Date('2024-02-20'),
    })
    await db.insert({
      name: 'Jane',
      email: 'jane@example.com',
      info: { age: 50 },
      active: false,
      createdAt: new Date('2024-03-10'),
    })
    await db.insert({
      name: 'Jack',
      email: 'jack@example.com',
      info: { age: 60 },
      active: true,
      createdAt: new Date('2024-04-05'),
    })
    await db.insert({
      name: 'Jill',
      email: 'jill@example.com',
      info: { age: 70 },
      active: null,
      createdAt: new Date('2024-05-01'),
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

  describe('get', () => {
    it('should get documents by exact index value', async () => {
      const results = await db.getIndex('name').get('David')

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('David')
      expect(results[0].email).to.equal('david@example.com')
    })

    it('should return empty array for non-existent value', async () => {
      const results = await db.getIndex('name').get('NonExistent')

      expect(results).to.have.lengthOf(0)
    })

    it('should get multiple documents with same index value', async () => {
      await db.insert({ name: 'David', email: 'david2@example.com', info: { age: 35 } })

      const results = await db.getIndex('name').get('David')

      expect(results).to.have.lengthOf(2)
      expect(results.map(r => r.email)).to.include.members(['david@example.com', 'david2@example.com'])
    })

    it('should query nested properties', async () => {
      const results = await db.getIndex('age').get(30)

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('David')
    })
  })

  describe('getOne', () => {
    it('should get first document by index value', async () => {
      const result = await db.getIndex('name').getOne('David')

      expect(result).to.not.be.null
      expect(result!.name).to.equal('David')
    })

    it('should return null for non-existent value', async () => {
      const result = await db.getIndex('name').getOne('NonExistent')

      expect(result).to.be.null
    })

    it('should return first document when multiple exist', async () => {
      await db.insert({ name: 'David', email: 'david2@example.com', info: { age: 35 } })

      const result = await db.getIndex('name').getOne('David')

      expect(result).to.not.be.null
      expect(result!.name).to.equal('David')
    })
  })

  describe('exists', () => {
    it('should return true for existing index value', async () => {
      const exists = await db.getIndex('name').exists('David')

      expect(exists).to.be.true
    })

    it('should return false for non-existent index value', async () => {
      const exists = await db.getIndex('name').exists('NonExistent')

      expect(exists).to.be.false
    })
  })

  describe('query - numeric ranges', () => {
    it('should query with gte operator', async () => {
      const results = await db.getIndex('age').query({ gte: 50 })

      expect(results).to.have.lengthOf(3)
      const ages = results.map(r => r.info.age)
      expect(ages).to.include.members([50, 60, 70])
    })

    it('should query with gt operator', async () => {
      const results = await db.getIndex('age').query({ gt: 50 })

      expect(results).to.have.lengthOf(2)
      const ages = results.map(r => r.info.age)
      expect(ages).to.include.members([60, 70])
    })

    it('should query with lte operator', async () => {
      const results = await db.getIndex('age').query({ lte: 40 })

      expect(results).to.have.lengthOf(2)
      const ages = results.map(r => r.info.age)
      expect(ages).to.include.members([30, 40])
    })

    it('should query with lt operator', async () => {
      const results = await db.getIndex('age').query({ lt: 40 })

      expect(results).to.have.lengthOf(1)
      expect(results[0].info.age).to.equal(30)
    })

    it('should query with range (gte and lte)', async () => {
      const results = await db.getIndex('age').query({ gte: 40, lte: 60 })

      expect(results).to.have.lengthOf(3)
      const ages = results.map(r => r.info.age)
      expect(ages).to.include.members([40, 50, 60])
    })

    it('should query with exclusive range (gt and lt)', async () => {
      const results = await db.getIndex('age').query({ gt: 40, lt: 70 })

      expect(results).to.have.lengthOf(2)
      const ages = results.map(r => r.info.age)
      expect(ages).to.include.members([50, 60])
    })
  })

  describe('query - eq and ne operators', () => {
    it('should query with eq operator', async () => {
      const results = await db.getIndex('age').query({ eq: 30 })

      expect(results).to.have.lengthOf(1)
      expect(results[0].info.age).to.equal(30)
    })

    it('should query with ne operator', async () => {
      const results = await db.getIndex('active').query({ ne: null })

      expect(results).to.have.lengthOf(4)
      const names = results.map(r => r.name)
      expect(names).to.not.include('Jill')
    })

    it('should query for null values with eq', async () => {
      const results = await db.getIndex('active').query({ eq: null })

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('Jill')
    })
  })

  describe('query - boolean values', () => {
    it('should query by true value', async () => {
      const results = await db.getIndex('active').query({ eq: true })

      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['David', 'John', 'Jack'])
    })

    it('should query by false value', async () => {
      const results = await db.getIndex('active').query({ eq: false })

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('Jane')
    })
  })

  describe('query - Date values', () => {
    it('should query dates with gte', async () => {
      const results = await db.getIndex('createdAt').query({
        gte: new Date('2024-03-01'),
      })

      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Jane', 'Jack', 'Jill'])
    })

    it('should query dates with range', async () => {
      const results = await db.getIndex('createdAt').query({
        gte: new Date('2024-02-01'),
        lte: new Date('2024-04-01'),
      })

      expect(results).to.have.lengthOf(2)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['John', 'Jane'])
    })
  })

  describe('query - string values', () => {
    it('should query strings with gte (lexicographic)', async () => {
      const results = await db.getIndex('name').query({ gte: 'Jane' })

      // Lexicographic order: David, Jack, Jane, Jill, John
      // gte 'Jane' returns: Jane, Jill, John
      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Jane', 'Jill', 'John'])
    })

    it('should query strings with lt (lexicographic)', async () => {
      const results = await db.getIndex('name').query({ lt: 'Jane' })

      // Lexicographic order: David, Jack, Jane, Jill, John
      // lt 'Jane' returns: David, Jack
      expect(results).to.have.lengthOf(2)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['David', 'Jack'])
    })
  })

  describe('index updates on edit', () => {
    it('should update index when document is edited', async () => {
      const id = (await db.getIndex('name').get('David'))[0].$id

      await db.edit(id, { name: 'David Updated' })

      const oldResults = await db.getIndex('name').get('David')
      expect(oldResults).to.have.lengthOf(0)

      const newResults = await db.getIndex('name').get('David Updated')
      expect(newResults).to.have.lengthOf(1)
      expect(newResults[0].$id).to.equal(id)
    })

    it('should update nested index when document is edited', async () => {
      const id = (await db.getIndex('age').get(30))[0].$id

      await db.edit(id, { info: { age: 35 } })

      const oldResults = await db.getIndex('age').get(30)
      expect(oldResults).to.have.lengthOf(0)

      const newResults = await db.getIndex('age').get(35)
      expect(newResults).to.have.lengthOf(1)
      expect(newResults[0].$id).to.equal(id)
    })
  })

  describe('index updates on delete', () => {
    it('should remove from index when document is deleted', async () => {
      const id = (await db.getIndex('name').get('David'))[0].$id

      await db.delete(id)

      const results = await db.getIndex('name').get('David')
      expect(results).to.have.lengthOf(0)

      const exists = await db.getIndex('name').exists('David')
      expect(exists).to.be.false
    })
  })

  describe('remapIndex', () => {
    it('should rebuild all indexes from scratch', async () => {
      // Manually corrupt index by deleting it
      if (fs.existsSync(TEST_DB_PATH)) {
        const files = fs.readdirSync(TEST_DB_PATH)
        files.forEach(file => {
          if (file.includes('index-')) {
            fs.rmSync(path.join(TEST_DB_PATH, file), { recursive: true, force: true })
          }
        })
      }

      // Remap indexes
      await db.remapIndex()

      // Verify indexes work
      const results = await db.getIndex('name').get('David')
      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('David')

      const ageResults = await db.getIndex('age').query({ gte: 50 })
      expect(ageResults).to.have.lengthOf(3)
    })
  })
})