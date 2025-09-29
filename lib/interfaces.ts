export type LocalDBIndexableType = string | number | Date | boolean | null | object;

export interface LocalDBEntity {}
export type LocalDBIdType = string

export type LocalDBEntityWithId<T> = T & {
  $id: LocalDBIdType
}

export interface LocalDBItterator<T = any> {
  gt?: T
  gte?: T
  lt?: T
  lte?: T
  eq?: T
  ne?: T
}