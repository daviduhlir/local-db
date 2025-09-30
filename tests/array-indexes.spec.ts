import { expect } from 'chai'
import { JsonDBRepository } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-array-indexes')

describe('Array Index Query Tests', () => {
  let db: JsonDBRepository<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        tags: {
          path: 'tags',
        },
        categories: {
          path: 'categories',
        },
      },
    })
    await db.open()

    // Insert test data with array fields
    await db.insert({
      name: 'Post 1',
      tags: ['javascript', 'typescript', 'nodejs'],
      categories: ['programming', 'web'],
    })
    await db.insert({
      name: 'Post 2',
      tags: ['python', 'django'],
      categories: ['programming', 'backend'],
    })
    await db.insert({
      name: 'Post 3',
      tags: ['javascript', 'react'],
      categories: ['programming', 'frontend'],
    })
    await db.insert({
      name: 'Post 4',
      tags: [],
      categories: ['uncategorized'],
    })
    await db.insert({
      name: 'Post 5',
      tags: ['typescript', 'react', 'nextjs'],
      categories: [],
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

  describe('query - includes operator', () => {
    it('should find documents that include a specific value in array', async () => {
      const results = await db.getIndex('tags').query({ includes: 'javascript' })

      expect(results).to.have.lengthOf(2)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 3'])
    })

    it('should find documents that include typescript', async () => {
      const results = await db.getIndex('tags').query({ includes: 'typescript' })

      expect(results).to.have.lengthOf(2)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 5'])
    })

    it('should return empty array when no documents include the value', async () => {
      const results = await db.getIndex('tags').query({ includes: 'nonexistent' })

      expect(results).to.have.lengthOf(0)
    })

    it('should work with categories index', async () => {
      const results = await db.getIndex('categories').query({ includes: 'programming' })

      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 2', 'Post 3'])
    })
  })

  describe('query - excludes operator', () => {
    it('should find documents that do not include a specific value in array', async () => {
      const results = await db.getIndex('tags').query({ excludes: 'javascript' })

      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 2', 'Post 4', 'Post 5'])
      expect(names).to.not.include('Post 1')
      expect(names).to.not.include('Post 3')
    })

    it('should find documents that do not include typescript', async () => {
      const results = await db.getIndex('tags').query({ excludes: 'typescript' })

      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 2', 'Post 3', 'Post 4'])
    })

    it('should return all documents when no documents include the value', async () => {
      const results = await db.getIndex('tags').query({ excludes: 'nonexistent' })

      expect(results).to.have.lengthOf(5)
    })
  })

  describe('query - empty operator', () => {
    it('should find documents with empty arrays', async () => {
      const results = await db.getIndex('tags').query({ empty: true })

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('Post 4')
      expect(results[0].tags).to.be.an('array').that.is.empty
    })

    it('should find documents with non-empty arrays when empty is false', async () => {
      const results = await db.getIndex('tags').query({ empty: false })

      expect(results).to.have.lengthOf(4)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 2', 'Post 3', 'Post 5'])
    })

    it('should work with categories index', async () => {
      const results = await db.getIndex('categories').query({ empty: true })

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('Post 5')
      expect(results[0].categories).to.be.an('array').that.is.empty
    })
  })

  describe('query - notEmpty operator', () => {
    it('should find documents with non-empty arrays', async () => {
      const results = await db.getIndex('tags').query({ notEmpty: true })

      expect(results).to.have.lengthOf(4)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 2', 'Post 3', 'Post 5'])
      expect(names).to.not.include('Post 4')
    })

    it('should find documents with empty arrays when notEmpty is false', async () => {
      const results = await db.getIndex('tags').query({ notEmpty: false })

      expect(results).to.have.lengthOf(1)
      expect(results[0].name).to.equal('Post 4')
    })

    it('should work with categories index', async () => {
      const results = await db.getIndex('categories').query({ notEmpty: true })

      expect(results).to.have.lengthOf(4)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 2', 'Post 3', 'Post 4'])
      expect(names).to.not.include('Post 5')
    })
  })

  describe('query - combined array operators', () => {
    it('should combine includes with notEmpty', async () => {
      const results = await db.getIndex('tags').query({
        includes: 'javascript',
        notEmpty: true,
      })

      expect(results).to.have.lengthOf(2)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 3'])
    })

    it('should combine excludes with notEmpty', async () => {
      const results = await db.getIndex('tags').query({
        excludes: 'javascript',
        notEmpty: true,
      })

      expect(results).to.have.lengthOf(2)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 2', 'Post 5'])
    })

    it('should handle includes with empty arrays excluded', async () => {
      const results = await db.getIndex('categories').query({
        includes: 'programming',
        notEmpty: true,
      })

      expect(results).to.have.lengthOf(3)
      const names = results.map(r => r.name)
      expect(names).to.include.members(['Post 1', 'Post 2', 'Post 3'])
    })
  })

  describe('array index updates on edit', () => {
    it('should update array index when document is edited', async () => {
      const id = (await db.getIndex('tags').query({ includes: 'javascript' }))[0].$id

      await db.edit(id, { tags: ['go', 'rust'] })

      const oldResults = await db.getIndex('tags').query({ includes: 'javascript' })
      expect(oldResults).to.have.lengthOf(1) // Only Post 3 remains

      const newResults = await db.getIndex('tags').query({ includes: 'go' })
      expect(newResults).to.have.lengthOf(1)
      expect(newResults[0].$id).to.equal(id)
    })

    it('should handle changing from non-empty to empty array', async () => {
      const id = (await db.getIndex('tags').query({ includes: 'javascript' }))[0].$id

      await db.edit(id, { tags: [] })

      const emptyResults = await db.getIndex('tags').query({ empty: true })
      expect(emptyResults).to.have.lengthOf(2) // Post 4 and the edited one
      expect(emptyResults.map(r => r.$id)).to.include(id)
    })

    it('should handle changing from empty to non-empty array', async () => {
      const id = (await db.getIndex('tags').query({ empty: true }))[0].$id

      await db.edit(id, { tags: ['newTag'] })

      const emptyResults = await db.getIndex('tags').query({ empty: true })
      expect(emptyResults).to.have.lengthOf(0)

      const nonEmptyResults = await db.getIndex('tags').query({ notEmpty: true })
      expect(nonEmptyResults.map(r => r.$id)).to.include(id)
    })
  })
})
