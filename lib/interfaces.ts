export type LocalDBIndexableType = string | number | symbol;

export interface LocalDbEntity {}
export type LocalDbIdType = string

export type LocalDbEntityWithId<T> = T & {
  $id: LocalDbIdType
}
