import { expect } from 'chai'
import { JsonDBRepository } from '../dist'
import * as path from 'path'
import * as fs from 'fs'

const TEST_DB_PATH = path.join(__dirname, '.test-db-index-creation')

describe('Index Creation Tests', () => {
  afterEach(() => {
    // Clean up test database
    if (fs.existsSync(TEST_DB_PATH)) {
      fs.rmSync(TEST_DB_PATH, { recursive: true, force: true })
    }
  })

  it('should create indexes retroactively when opening DB with new index definitions', async function() {
    this.timeout(10000)

    // Step 1: Create DB without indexes and insert data
    const dbWithoutIndexes = new JsonDBRepository(TEST_DB_PATH)
    await dbWithoutIndexes.open()

    const id1 = await dbWithoutIndexes.insert({
      name: 'David',
      age: 30,
      status: 'active',
    })

    const id2 = await dbWithoutIndexes.insert({
      name: 'John',
      age: 40,
      status: 'inactive',
    })

    const id3 = await dbWithoutIndexes.insert({
      name: 'Jane',
      age: 50,
      status: 'active',
    })

    await dbWithoutIndexes.close()

    // Step 2: Reopen DB with index definitions
    const dbWithIndexes = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        name: {
          path: 'name',
        },
        age: {
          path: 'age',
        },
        status: {
          path: 'status',
        },
      },
    })

    await dbWithIndexes.open()

    // Step 3: Verify indexes work - query by name
    const davidResults: any = await dbWithIndexes.getIndex('name').get('David')
    expect(davidResults).to.have.lengthOf(1)
    expect(davidResults[0].name).to.equal('David')
    expect(davidResults[0].age).to.equal(30)

    // Step 4: Verify indexes work - query by age range
    const ageRangeResults: any = await dbWithIndexes.getIndex('age').query({
      gte: 35,
      lte: 50,
    })
    expect(ageRangeResults).to.have.lengthOf(2)
    const names = ageRangeResults.map((r: any) => r.name)
    expect(names).to.include.members(['John', 'Jane'])

    // Step 5: Verify indexes work - query by status
    const activeResults: any = await dbWithIndexes.getIndex('status').get('active')
    expect(activeResults).to.have.lengthOf(2)
    const activeNames = activeResults.map((r: any) => r.name)
    expect(activeNames).to.include.members(['David', 'Jane'])

    await dbWithIndexes.close()
  })

  it('should handle adding new indexes to existing DB with data', async function() {
    this.timeout(10000)

    // Step 1: Create DB with one index
    const dbWithOneIndex = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        name: {
          path: 'name',
        },
      },
    })
    await dbWithOneIndex.open()

    await dbWithOneIndex.insert({
      name: 'David',
      age: 30,
      status: 'active',
      city: 'Prague',
    })

    await dbWithOneIndex.insert({
      name: 'John',
      age: 40,
      status: 'inactive',
      city: 'London',
    })

    await dbWithOneIndex.close()

    // Step 2: Reopen with additional indexes
    const dbWithMoreIndexes = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        name: {
          path: 'name',
        },
        age: {
          path: 'age',
        },
        city: {
          path: 'city',
        },
      },
    })

    await dbWithMoreIndexes.open()

    // Step 3: Verify old index still works
    const davidResults = await dbWithMoreIndexes.getIndex('name').get('David')
    expect(davidResults).to.have.lengthOf(1)

    // Step 4: Verify new indexes work
    const ageResults: any = await dbWithMoreIndexes.getIndex('age').get(30)
    expect(ageResults).to.have.lengthOf(1)
    expect(ageResults[0].name).to.equal('David')

    const pragueResults: any = await dbWithMoreIndexes.getIndex('city').get('Prague')
    expect(pragueResults).to.have.lengthOf(1)
    expect(pragueResults[0].name).to.equal('David')

    await dbWithMoreIndexes.close()
  })

  it('should handle nested property indexes on existing data', async function() {
    this.timeout(10000)

    // Step 1: Create DB without indexes
    const dbWithoutIndexes = new JsonDBRepository(TEST_DB_PATH)
    await dbWithoutIndexes.open()

    await dbWithoutIndexes.insert({
      name: 'David',
      info: {
        age: 30,
        address: {
          city: 'Prague',
        },
      },
    })

    await dbWithoutIndexes.insert({
      name: 'John',
      info: {
        age: 40,
        address: {
          city: 'London',
        },
      },
    })

    await dbWithoutIndexes.close()

    // Step 2: Reopen with nested indexes
    const dbWithIndexes = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        age: {
          path: 'info.age',
        },
        city: {
          path: 'info.address.city',
        },
      },
    })

    await dbWithIndexes.open()

    // Step 3: Verify nested indexes work
    const ageResults: any = await dbWithIndexes.getIndex('age').get(30)
    expect(ageResults).to.have.lengthOf(1)
    expect(ageResults[0].name).to.equal('David')

    const cityResults: any = await dbWithIndexes.getIndex('city').get('Prague')
    expect(cityResults).to.have.lengthOf(1)
    expect(cityResults[0].name).to.equal('David')

    await dbWithIndexes.close()
  })

  it('should handle large dataset when creating indexes retroactively', async function() {
    this.timeout(15000)

    // Step 1: Insert 100 documents without indexes
    const dbWithoutIndexes = new JsonDBRepository(TEST_DB_PATH)
    await dbWithoutIndexes.open()

    for (let i = 0; i < 100; i++) {
      await dbWithoutIndexes.insert({
        name: `User ${i}`,
        counter: i,
        status: i % 2 === 0 ? 'active' : 'inactive',
      })
    }

    await dbWithoutIndexes.close()

    // Step 2: Reopen with indexes
    const dbWithIndexes = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        counter: {
          path: 'counter',
        },
        status: {
          path: 'status',
        },
      },
    })

    await dbWithIndexes.open()

    // Step 3: Verify indexes work on all data
    const activeResults = await dbWithIndexes.getIndex('status').get('active')
    expect(activeResults).to.have.lengthOf(50)

    const inactiveResults = await dbWithIndexes.getIndex('status').get('inactive')
    expect(inactiveResults).to.have.lengthOf(50)

    // Step 4: Verify range queries work
    const rangeResults = await dbWithIndexes.getIndex('counter').query({
      gte: 40,
      lte: 60,
    })
    expect(rangeResults).to.have.lengthOf(21) // 40 to 60 inclusive

    await dbWithIndexes.close()
  })

  it('should handle null and undefined values when creating indexes retroactively', async function() {
    this.timeout(10000)

    // Step 1: Insert data with null/undefined values
    const dbWithoutIndexes = new JsonDBRepository(TEST_DB_PATH)
    await dbWithoutIndexes.open()

    await dbWithoutIndexes.insert({
      name: 'David',
      status: 'active',
    })

    await dbWithoutIndexes.insert({
      name: 'John',
      status: null,
    })

    await dbWithoutIndexes.insert({
      name: 'Jane',
      status: undefined,
    })

    await dbWithoutIndexes.close()

    // Step 2: Reopen with index on status
    const dbWithIndexes = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        status: {
          path: 'status',
        },
      },
    })

    await dbWithIndexes.open()

    // Step 3: Verify null values are indexed
    const nullResults: any = await dbWithIndexes.getIndex('status').get(null)
    expect(nullResults).to.have.lengthOf(1)
    expect(nullResults[0].name).to.equal('John')

    // Step 4: Verify undefined values are indexed
    const undefinedResults: any = await dbWithIndexes.getIndex('status').get(undefined)
    expect(undefinedResults).to.have.lengthOf(1)
    expect(undefinedResults[0].name).to.equal('Jane')

    // Step 5: Verify ne operator works
    const notNullResults = await dbWithIndexes.getIndex('status').query({ ne: null })
    expect(notNullResults.length).to.be.at.least(1)

    await dbWithIndexes.close()
  })

  it('should allow inserting new data after indexes are created retroactively', async function() {
    this.timeout(10000)

    // Step 1: Create DB without indexes and add initial data
    const dbWithoutIndexes = new JsonDBRepository(TEST_DB_PATH)
    await dbWithoutIndexes.open()

    await dbWithoutIndexes.insert({
      name: 'David',
      age: 30,
    })

    await dbWithoutIndexes.close()

    // Step 2: Reopen with indexes
    const dbWithIndexes = new JsonDBRepository(TEST_DB_PATH, {
      indexes: {
        name: {
          path: 'name',
        },
        age: {
          path: 'age',
        },
      },
    })

    await dbWithIndexes.open()

    // Step 3: Insert new data
    await dbWithIndexes.insert({
      name: 'John',
      age: 40,
    })

    // Step 4: Verify both old and new data are indexed
    const davidResults = await dbWithIndexes.getIndex('name').get('David')
    expect(davidResults).to.have.lengthOf(1)

    const johnResults = await dbWithIndexes.getIndex('name').get('John')
    expect(johnResults).to.have.lengthOf(1)

    // Step 5: Verify range query includes all data
    const ageResults = await dbWithIndexes.getIndex('age').query({ gte: 25 })
    expect(ageResults).to.have.lengthOf(2)

    await dbWithIndexes.close()
  })
})