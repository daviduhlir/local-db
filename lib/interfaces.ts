export type LocalDBIndexableType = string | number | Date | boolean | null | object;

export interface LocalDbEntity {}
export type LocalDbIdType = string

export type LocalDbEntityWithId<T> = T & {
  $id: LocalDbIdType
}
