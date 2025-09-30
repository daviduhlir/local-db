export type JsonDBIndexableType = string | number | Date | boolean | null | object

export interface JsonDBEntity {}
export type JsonDBIdType = string

export type JsonDBEntityWithId<T> = T & {
  $id: JsonDBIdType
}

export interface JsonDBItterator<T = any> {
  gt?: T
  gte?: T
  lt?: T
  lte?: T
  eq?: T
  ne?: T
  in?: T[]
  nin?: T[]

  // for array types
  includes?: T
  excludes?: T
  empty?: boolean
  notEmpty?: boolean
}
