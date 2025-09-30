import { JsonDBRepository } from '@david.uhlir/local-db'

const db = new JsonDBRepository('./db', {
  indexes: {
    name: {
      path: 'name',
    },
    age: {
      path: 'info.age',
    },
    test: {
      path: 'test',
    },
  },
})


async function main() {
  await db.open()

  await db.insert({
    name: 'David',
    info: {
      age: 30,
    },
    test: 'Mr. David',
  })
  await db.insert({
    name: 'John',
    info: {
      age: 40,
    },
    test: 'Mr. John',
  })
  await db.insert({
    name: 'Jane',
    info: {
      age: 50,
    },
    test: 'Ms. Jane',
  })
  await db.insert({
    name: 'Jack',
    info: {
      age: 60,
    },
    test: 'Mr. Jack',
  })
  await db.insert({
    name: 'Jill',
    info: {
      age: 70,
    },
    test: null,
  })
  const data = await db.getIndex('name').get('David')
  console.log('Data1', data)
  const data2 = await db.getIndex('age').query({
    gte: 40,
    lte: 60,
  })
  console.log('Data2', data2)

  const data3 = await db.getIndex('test').query({
    ne: null,
  })
  console.log('Data3', data3)

}

main()