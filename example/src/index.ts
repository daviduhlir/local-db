import { LocalDB } from '@david.uhlir/local-db'

const db = new LocalDB('./db', {
  indexes: {
    name: 'name',
    age: 'age',
  },
})


async function main() {
  await db.insert({
    name: 'David',
    age: 30,
  })
  await db.insert({
    name: 'John',
    age: 40,
  })
  await db.insert({
    name: 'Jane',
    age: 50,
  })
  await db.insert({
    name: 'Jack',
    age: 60,
  })
  await db.insert({
    name: 'Jill',
    age: 70,
  })
  const data = await db.getIndex('name').find('David')
  console.log(data)
}

main()