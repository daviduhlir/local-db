import { expect } from 'chai'
import { LocalDB } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-edge-cases')

describe('Edge Cases Tests', () => {
  let db: LocalDB<any, any>

  beforeEach(async () => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }

    db = new LocalDB(TEST_DB_PATH, {
      indexes: {
        value: {
          path: 'value',
        },
        nested: {
          path: 'deep.nested.value',
        },
        array: {
          path: 'items',
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

  describe('empty and null values', () => {
    it('should handle empty string', async () => {
      const id = await db.insert({ value: '' })
      const doc = await db.getOne(id)

      expect(doc!.value).to.equal('')

      const results = await db.getIndex('value').get('')
      expect(results).to.have.lengthOf(1)
    })

    it('should handle null values', async () => {
      const id = await db.insert({ value: null })
      const doc = await db.getOne(id)

      expect(doc!.value).to.be.null

      const results = await db.getIndex('value').get(null)
      expect(results).to.have.lengthOf(1)
    })

    it('should handle undefined values', async () => {
      const id = await db.insert({ value: undefined })
      const doc = await db.getOne(id)

      expect(doc!.value).to.be.undefined

      const results = await db.getIndex('value').get(undefined)
      expect(results).to.have.lengthOf(1)
    })

    it('should handle missing nested properties', async () => {
      const id = await db.insert({ deep: {} })
      const doc = await db.getOne(id)

      expect(doc!.deep).to.deep.equal({})

      const results = await db.getIndex('nested').get(undefined)
      expect(results).to.have.lengthOf(1)
    })

    it('should handle completely missing nested path', async () => {
      const id = await db.insert({ other: 'data' })
      const doc = await db.getOne(id)

      expect(doc!.deep).to.be.undefined

      const results = await db.getIndex('nested').get(undefined)
      expect(results).to.have.lengthOf(1)
    })
  })

  describe('special characters and unicode', () => {
    it('should handle special characters in strings', async () => {
      const specialStrings = [
        'hello@world.com',
        'user/name',
        'data\\path',
        'key=value',
        'a:b:c',
        'test|pipe',
        'question?',
        'exclamation!',
        'hash#tag',
        'dollar$sign',
        'percent%',
        'ampersand&',
        'asterisk*',
        'plus+minus-',
      ]

      for (const str of specialStrings) {
        const id = await db.insert({ value: str })
        const results = await db.getIndex('value').get(str)
        expect(results).to.have.lengthOf(1)
        expect(results[0].value).to.equal(str)
      }
    })

    it('should handle unicode characters', async () => {
      const unicodeStrings = [
        '你好世界',
        'Привет мир',
        '🎉🚀💻',
        'café',
        'naïve',
        'Ñoño',
        '日本語',
        '한글',
        '🔥💯',
      ]

      for (const str of unicodeStrings) {
        const id = await db.insert({ value: str })
        const results = await db.getIndex('value').get(str)
        expect(results).to.have.lengthOf(1)
        expect(results[0].value).to.equal(str)
      }
    })

    it('should handle very long strings', async () => {
      const longString = 'a'.repeat(10000)
      const id = await db.insert({ value: longString })
      const results = await db.getIndex('value').get(longString)

      expect(results).to.have.lengthOf(1)
      expect(results[0].value).to.have.lengthOf(10000)
    })

    it('should handle strings with newlines and tabs', async () => {
      const multilineString = 'line1\nline2\tindented\r\nline3'
      const id = await db.insert({ value: multilineString })
      const results = await db.getIndex('value').get(multilineString)

      expect(results).to.have.lengthOf(1)
      expect(results[0].value).to.equal(multilineString)
    })
  })

  describe('numeric edge cases', () => {
    it('should handle zero', async () => {
      const id = await db.insert({ value: 0 })
      const results = await db.getIndex('value').get(0)

      expect(results).to.have.lengthOf(1)
      expect(results[0].value).to.equal(0)
    })

    it('should handle negative numbers', async () => {
      await db.insert({ value: -100 })
      await db.insert({ value: -50 })
      await db.insert({ value: 0 })
      await db.insert({ value: 50 })
      await db.insert({ value: 100 })

      const results = await db.getIndex('value').query({ gte: -50, lte: 50 })
      expect(results).to.have.lengthOf(3)
    })

    it('should handle very large numbers', async () => {
      const largeNumber = Number.MAX_SAFE_INTEGER
      const id = await db.insert({ value: largeNumber })
      const results = await db.getIndex('value').get(largeNumber)

      expect(results).to.have.lengthOf(1)
      expect(results[0].value).to.equal(largeNumber)
    })

    it('should handle very small numbers', async () => {
      const smallNumber = Number.MIN_SAFE_INTEGER
      const id = await db.insert({ value: smallNumber })
      const results = await db.getIndex('value').get(smallNumber)

      expect(results).to.have.lengthOf(1)
      expect(results[0].value).to.equal(smallNumber)
    })

    it('should handle decimal numbers', async () => {
      const decimals = [0.1, 0.2, 0.3, 1.5, 3.14159, 2.71828]

      for (const num of decimals) {
        await db.insert({ value: num })
      }

      const results = await db.getIndex('value').query({ gte: 0.2, lte: 3.0 })
      expect(results).to.have.lengthOf(4) // 0.2, 0.3, 1.5, 2.71828
    })

    it('should handle Infinity (converts to null in JSON)', async () => {
      const id = await db.insert({ value: Infinity })
      const doc = await db.getOne(id)

      // JSON doesn't support Infinity, it converts to null
      expect(doc!.value).to.be.null
    })

    it('should handle -Infinity (converts to null in JSON)', async () => {
      const id = await db.insert({ value: -Infinity })
      const doc = await db.getOne(id)

      // JSON doesn't support -Infinity, it converts to null
      expect(doc!.value).to.be.null
    })
  })

  describe('date edge cases', () => {
    it('should handle epoch date', async () => {
      const epoch = new Date(0)
      const id = await db.insert({ value: epoch })
      const results = await db.getIndex('value').get(epoch)

      expect(results).to.have.lengthOf(1)
    })

    it('should handle very old dates', async () => {
      const oldDate = new Date('1900-01-01')
      const id = await db.insert({ value: oldDate })
      const results = await db.getIndex('value').get(oldDate)

      expect(results).to.have.lengthOf(1)
    })

    it('should handle far future dates', async () => {
      const futureDate = new Date('2100-12-31')
      const id = await db.insert({ value: futureDate })
      const results = await db.getIndex('value').get(futureDate)

      expect(results).to.have.lengthOf(1)
    })

    it('should handle invalid dates (converts to null in JSON)', async () => {
      const invalidDate = new Date('invalid')
      const id = await db.insert({ value: invalidDate })
      const doc = await db.getOne(id)

      // Invalid dates (NaN) convert to null in JSON
      expect(doc!.value).to.be.null
    })
  })

  describe('complex object types', () => {
    it('should handle nested objects', async () => {
      const complex = {
        level1: {
          level2: {
            level3: {
              value: 'deep',
            },
          },
        },
      }

      const id = await db.insert(complex)
      const doc = await db.getOne(id)

      expect(doc!.level1.level2.level3.value).to.equal('deep')
    })

    it('should handle arrays', async () => {
      const withArray = {
        items: [1, 2, 3, 4, 5],
      }

      const id = await db.insert(withArray)
      const doc = await db.getOne(id)

      expect(doc!.items).to.deep.equal([1, 2, 3, 4, 5])
    })

    it('should handle arrays of objects', async () => {
      const complexArray = {
        users: [
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 },
        ],
      }

      const id = await db.insert(complexArray)
      const doc = await db.getOne(id)

      expect(doc!.users).to.have.lengthOf(2)
      expect(doc!.users[0].name).to.equal('Alice')
    })

    it('should handle mixed type arrays', async () => {
      const mixed = {
        items: [1, 'two', true, null, { key: 'value' }, [1, 2]],
      }

      const id = await db.insert(mixed)
      const doc = await db.getOne(id)

      expect(doc!.items).to.have.lengthOf(6)
    })

    it('should handle empty objects', async () => {
      const id = await db.insert({})
      const doc = await db.getOne(id)

      expect(doc!.$id).to.exist
      expect(Object.keys(doc!).filter(k => k !== '$id')).to.have.lengthOf(0)
    })

    it('should handle empty arrays', async () => {
      const withEmptyArray = { items: [] }
      const id = await db.insert(withEmptyArray)
      const doc = await db.getOne(id)

      expect(doc!.items).to.deep.equal([])
    })
  })

  describe('large datasets', () => {
    it('should handle many documents with same index value', async () => {
      const promises: any[] = []
      for (let i = 0; i < 100; i++) {
        promises.push(db.insert({ value: 'same' }))
      }

      await Promise.all(promises)

      const results = await db.getIndex('value').get('same')
      expect(results).to.have.lengthOf(100)
    })

    it('should handle documents with many properties', async () => {
      const manyProps: any = {}
      for (let i = 0; i < 100; i++) {
        manyProps[`prop${i}`] = `value${i}`
      }

      const id = await db.insert(manyProps)
      const doc = await db.getOne(id)

      expect(Object.keys(doc!).filter(k => k !== '$id')).to.have.lengthOf(100)
    })
  })

  describe('edit edge cases', () => {
    it('should handle editing non-existent properties', async () => {
      const id = await db.insert({ a: 1 })
      await db.edit(id, { b: 2 } as any)

      const doc = await db.getOne(id)
      expect(doc!.a).to.equal(1)
      expect(doc!.b).to.equal(2)
    })

    it('should handle deep merge on edit', async () => {
      const id = await db.insert({
        user: {
          name: 'Alice',
          age: 30,
        },
      })

      await db.edit(id, {
        user: {
          age: 31,
        },
      } as any)

      const doc = await db.getOne(id)
      // Note: spread operator does shallow merge, so nested objects get replaced
      expect(doc!.user.age).to.equal(31)
      expect(doc!.user.name).to.be.undefined // This is expected with shallow merge
    })

    it('should handle editing to null', async () => {
      const id = await db.insert({ value: 'test' })
      await db.edit(id, { value: null })

      const doc = await db.getOne(id)
      expect(doc!.value).to.be.null
    })

    it('should handle editing to undefined', async () => {
      const id = await db.insert({ value: 'test' })
      await db.edit(id, { value: undefined })

      const doc = await db.getOne(id)
      expect(doc!.value).to.be.undefined
    })
  })

  describe('query edge cases', () => {
    it('should handle query with no results', async () => {
      await db.insert({ value: 1 })
      await db.insert({ value: 2 })

      const results = await db.getIndex('value').query({ gt: 10 })
      expect(results).to.have.lengthOf(0)
    })

    it('should handle query with all documents matching', async () => {
      for (let i = 0; i < 10; i++) {
        await db.insert({ value: i })
      }

      const results = await db.getIndex('value').query({ gte: 0 })
      expect(results).to.have.lengthOf(10)
    })

    it('should handle boundary values in range queries', async () => {
      await db.insert({ value: 10 })
      await db.insert({ value: 20 })
      await db.insert({ value: 30 })

      const results = await db.getIndex('value').query({ gte: 10, lte: 30 })
      expect(results).to.have.lengthOf(3)

      const exclusive = await db.getIndex('value').query({ gt: 10, lt: 30 })
      expect(exclusive).to.have.lengthOf(1)
    })
  })

  describe('database without indexes', () => {
    it('should work without any indexes defined', async () => {
      const simpleDb = new LocalDB(path.join(TEST_DB_PATH, 'simple'))

      const id = await simpleDb.insert({ value: 'test' })
      const doc: any = await simpleDb.getOne(id)

      expect(doc!.value).to.equal('test')

      await simpleDb.close()
    })
  })

  describe('property name edge cases', () => {
    it('should handle properties with special names', async () => {
      const id = await db.insert({
        constructor: 'test1',
        prototype: 'test2',
        __proto__: 'test3',
        toString: 'test4',
      })

      const doc = await db.getOne(id)
      expect(doc!.constructor).to.equal('test1')
      expect(doc!.prototype).to.equal('test2')
      expect(doc!.toString).to.equal('test4')
    })

    it('should handle $id property in input data', async () => {
      const id = await db.insert({ $id: 'custom-id', value: 'test' } as any)

      const doc = await db.getOne(id)
      // The database generates its own ID, so custom $id should be overwritten
      expect(doc!.$id).to.equal(id)
      expect(doc!.$id).to.not.equal('custom-id')
    })
  })
})