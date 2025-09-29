# @david.uhlir/local-db

A simple, lightweight local JSON database built on LevelDB with support for indexes and queries. Perfect for Node.js applications that need persistent storage without the complexity of a full database system.

## Features

- **Simple API** - Easy-to-use interface for CRUD operations
- **Indexed queries** - Create indexes on nested object properties for fast lookups
- **Range queries** - Support for gt, gte, lt, lte, eq, ne operators
- **TypeScript support** - Full type definitions included
- **LevelDB powered** - Reliable, embedded database storage
- **Nested property indexing** - Index any nested property using dot notation

## Installation

```bash
npm install @david.uhlir/local-db
```

## Quick Start

```typescript
import { LocalDB } from '@david.uhlir/local-db'

// Create a database with indexes
const db = new LocalDB('./db', {
  indexes: {
    name: {
      path: 'name',
    },
    age: {
      path: 'info.age',
    },
  },
})

// Insert data
await db.insert({
  name: 'David',
  info: {
    age: 30,
  },
})

// Query by index
const users = await db.getIndex('name').get('David')
const ageRange = await db.getIndex('age').query({
  gte: 25,
  lte: 35,
})
```

## API Reference

### LocalDB Constructor

```typescript
new LocalDB<T, I>(dbPath: string, options?: LocalDBOptions<I>)
```

**Parameters:**
- `dbPath` - Path to the database directory
- `options.indexes` - Object defining indexes:
  - `path` - Dot-notation path to the property to index (e.g., `'info.age'`)

### Methods

#### `insert(data: T): Promise<string>`

Insert a new document into the database. Returns the generated ID.

```typescript
const id = await db.insert({
  name: 'John',
  info: { age: 40 },
})
```

#### `getOne(id: string): Promise<LocalDBEntityWithId<T> | null>`

Get a single document by ID.

```typescript
const user = await db.getOne(id)
```

#### `get(ids: string[]): Promise<LocalDBEntityWithId<T>[]>`

Get multiple documents by IDs.

```typescript
const users = await db.get([id1, id2, id3])
```

#### `exists(id: string): Promise<boolean>`

Check if a document exists.

```typescript
const exists = await db.exists(id)
```

#### `edit(id: string, data: Partial<T>): Promise<void>`

Update a document. Automatically updates all indexes.

```typescript
await db.edit(id, {
  info: { age: 31 },
})
```

#### `delete(id: string): Promise<void>`

Delete a document. Automatically updates all indexes.

```typescript
await db.delete(id)
```

#### `getIndex(indexName: string): LocalDBIndexGetter<T>`

Get an index accessor for querying.

```typescript
const nameIndex = db.getIndex('name')
```

#### `remapIndex(): Promise<void>`

Rebuild all indexes from scratch. Useful if index definitions change.

```typescript
await db.remapIndex()
```

### Index Query Methods

Once you get an index using `getIndex()`, you can use these methods:

#### `get(value: any): Promise<LocalDBEntityWithId<T>[]>`

Get all documents where the indexed field equals the value.

```typescript
const davids = await db.getIndex('name').get('David')
```

#### `getOne(value: any): Promise<LocalDBEntityWithId<T> | null>`

Get the first document where the indexed field equals the value.

```typescript
const david = await db.getIndex('name').getOne('David')
```

#### `exists(value: any): Promise<boolean>`

Check if any document has the indexed field equal to the value.

```typescript
const hasDavid = await db.getIndex('name').exists('David')
```

#### `query(options: LocalDBIterator): Promise<LocalDBEntityWithId<T>[]>`

Query documents using range operators.

**Query operators:**
- `gt` - Greater than
- `gte` - Greater than or equal
- `lt` - Less than
- `lte` - Less than or equal
- `eq` - Equal to
- `ne` - Not equal to

```typescript
// Age between 40 and 60
const midAge = await db.getIndex('age').query({
  gte: 40,
  lte: 60,
})

// Not null
const withTest = await db.getIndex('test').query({
  ne: null,
})
```

## Example

See the complete example in the `example/` directory:

```typescript
import { LocalDB } from '@david.uhlir/local-db'

const db = new LocalDB('./db', {
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
  // Insert multiple documents
  await db.insert({
    name: 'David',
    info: { age: 30 },
    test: 'Mr. David',
  })

  await db.insert({
    name: 'John',
    info: { age: 40 },
    test: 'Mr. John',
  })

  await db.insert({
    name: 'Jane',
    info: { age: 50 },
    test: 'Ms. Jane',
  })

  // Get by exact value
  const david = await db.getIndex('name').get('David')
  console.log('Found:', david)

  // Query by range
  const adults = await db.getIndex('age').query({
    gte: 40,
    lte: 60,
  })
  console.log('Adults 40-60:', adults)

  // Query not null
  const withTest = await db.getIndex('test').query({
    ne: null,
  })
  console.log('Has test field:', withTest)
}

main()
```

Run the example:

```bash
cd example
npm install
npm start
```

## TypeScript Support

The library is written in TypeScript and includes full type definitions. You can define your document type:

```typescript
interface User {
  name: string
  email: string
  info: {
    age: number
    city: string
  }
}

const db = new LocalDB<User, typeof indexes>('./db', {
  indexes: {
    name: { path: 'name' },
    email: { path: 'email' },
    age: { path: 'info.age' },
  } as const,
})
```

Documents returned from the database will include a `$id` field:

```typescript
type UserWithId = User & { $id: string }
```

## Indexable Types

The following types can be indexed and queried:

- `string`
- `number`
- `boolean`
- `Date`
- `null`
- `object` (hashed)

## Performance Tips

1. **Create indexes** for fields you frequently query
2. **Use `getOne()`** instead of `get()` when you only need one result
3. **Batch operations** when inserting multiple documents
4. **Rebuild indexes** with `remapIndex()` if you change index definitions

## License

ISC

## Author

David Uhlíř

## Repository

https://github.com/daviduhlir/local-db