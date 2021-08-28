import { FilterSubExpressions } from '@vramework/generic/dist/filter'
import * as pg from 'pg'
import { QueryResult } from 'pg'

import { snakeCase } from 'snake-case'
import { createBulkInsert, createInsert, exactlyOneResult, getFilters, Logger, QueryInterface, sanitizeResult, ValueTypes } from './database-utils'
import { TypedPostgresPool } from './typed-postgres-pool'

export class TypedPostgresClient<Tables extends { [key: string]: any }, CustomTypes> {
  private client: pg.PoolClient | null = null

  constructor(private pool: TypedPostgresPool<Tables>, private logger: Logger, private userId?: string) {
  }

  public async closeSession() {
    if (this.client) {
      this.client.release()
    }
  }

  /**
   * 
   * @param table 
   * @param fields 
   * @param filters 
   */
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions): Promise<T[]>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions, notSingleError: Error): Promise<T>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<T | T[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<T>(`SELECT * FROM "app"."${table}" ${filter}`, filterValues)
    if (notSingleError) {
      return sanitizeResult(exactlyOneResult(result.rows, notSingleError))
    }
    return result.rows
  }

  /**
   * 
   * @param table 
   * @param fields 
   * @param filters 
   */
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions): Promise<Pick<T, typeof fields[number]>[]>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions, notSingleError: Error): Promise<Pick<T, typeof fields[number]>>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<Pick<T, typeof fields[number]> | Pick<T, typeof fields[number]>[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<Pick<T, typeof fields[number]>>(({ sf }) => `
      SELECT ${sf(table, fields, table as string)}
      FROM "app"."${table}"
      ${filter}
    `, filterValues)
    if (notSingleError) {
      return sanitizeResult(exactlyOneResult(result.rows, notSingleError))
    }
    return result.rows
  }

  /**
   * 
   * @param table 
   * @param insert 
   */
  public async crudBulkInsert<N extends keyof Tables, T extends Tables[N]>(table: N, insert: Partial<Record<keyof T, ValueTypes | CustomTypes>>[]): Promise<void>
  public async crudBulkInsert<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, insert: Partial<Record<keyof T, ValueTypes | CustomTypes>>[], returns: readonly (keyof T)[]): Promise<Record<keyof T, any>[]>
  public async crudBulkInsert<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, insert: Partial<Record<keyof T, ValueTypes | CustomTypes>>[], returns?: readonly (keyof T)[]): Promise<void | Record<keyof T, any>[]> {
    const [keys, values, realValues] = createBulkInsert(insert as any)
    if (returns) {
      const returnStatement = (returns || []).map(key => snakeCase(key.toString())).join(',')
      const result = await this.query<Pick<T, typeof returns[number]>>(`INSERT INTO "app".${table}(${keys}) VALUES ${values} RETURNING ${returnStatement};`, realValues)
      return result.rows
    } else {
      await this.query(`INSERT INTO "app".${table}(${keys}) VALUES ${values}`, realValues)
    }
  }

  /**
   * 
   * @param table 
   * @param insert 
   */
  public async crudInsert<N extends keyof Tables, T extends Tables[N]>(table: N, insert: Partial<Record<keyof T, ValueTypes | CustomTypes>>, returns?: []): Promise<void>
  public async crudInsert<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, insert: Partial<Record<keyof T, ValueTypes | CustomTypes>>, returns: F): Promise<Pick<T, typeof returns[number]>>
  public async crudInsert<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, insert: Partial<Record<keyof T, ValueTypes | CustomTypes>>, returns: F): Promise<void | Pick<T, typeof returns[number]>> {
    const [keys, values, realValues] = createInsert(insert as any)
    if (returns) {
      const returnStatement = returns.map(key => snakeCase(key.toString())).join(',')
      return await this.one<Pick<T, typeof returns[number]>>(`INSERT INTO "app".${table}(${keys}) VALUES (${values}) RETURNING ${returnStatement};`, realValues, new Error())
    } else {
      await this.query(`INSERT INTO "app".${table}(${keys}) VALUES (${values})`, realValues)
    }
  }

  /**
   * 
   * @param table 
   * @param update 
   * @param filters 
   * @param error 
   * @returns Void
   */
  public async crudUpdate<N extends keyof Tables, T extends Tables[N]>(table: N, update: Partial<T>, filters: Partial<T>, error?: Error): Promise<void> {
    if (Object.keys(update).length === 0) {
      return
    }
    const { filter, filterValues } = getFilters(filters)
    const [keys, values, realValues] = createInsert(update as any, filterValues.length)
    const result = await this.query(`
        UPDATE "app".${table}
        SET (${keys}) = row(${values})
        ${filter}
    `, [...filterValues, ...realValues])
    if (result.rowCount !== 1 && error) {
      throw error
    }
  }

  /**
   * 
   * @param table 
   * @param filters 
   * @param notSingleError 
   * @returns 
   */
  public async crudDelete<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, filters: Partial<T>): Promise<void>
  public async crudDelete<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, filters: Partial<T>, returns: F, notSingleError: Error): Promise<Pick<T, typeof returns[number]>>
  public async crudDelete<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, filters: Partial<T>, returns: F | [], notSingleError: Error): Promise<void | Pick<T, typeof returns[number]>>
  public async crudDelete<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, filters: Partial<T>, returns: F = [] as unknown as F, notSingleError?: Error): Promise<void | Pick<T, typeof returns[number]> | Pick<T, typeof returns[number]>[]> {
    const { filter, filterValues } = getFilters(filters)
    if (returns.length > 1) {
      const r = await this.query<Pick<T, typeof returns[number]>>(`DELETE FROM "app"."${table}" RETURNING ${returns.join(',')}, ${filter}`, filterValues)
      if (notSingleError) {
        return exactlyOneResult(r.rows, notSingleError)
      }
      return r.rows
    }

    const result = await this.query<void>(`DELETE FROM "app"."${table}", ${filter}`, filterValues)
    if (notSingleError && result.rowCount !== 1) {
      exactlyOneResult(result.rows, notSingleError)
    }
  }

  public async debugQuery<T = { rows: unknown[] }>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = []
  ): Promise<QueryResult<T>> {
    return await this._query<T>(statement, values, 'debug')
  }

  public async query<T = { rows: unknown[] }>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = []
  ): Promise<QueryResult<T>> {
    return await this._query<T>(statement, values)
  }

  public async one<T>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = [],
    error: Error
  ): Promise<T> {
    const r = await this._query<T>(statement, values)
    return exactlyOneResult(r.rows, error)
  }

  public async many<T>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = []
  ): Promise<T[]> {
    const r = await this._query<T>(statement, values)
    return r.rows
  }

  private async _query<T = { rows: unknown[] }>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = [],
    debug?: 'debug',
  ): Promise<QueryResult<T>> {
    let query = typeof statement === 'string' ? statement : statement({
      cf: this.createFields,
      sf: this.selectFields
    })
    query = query.replace(/^\s*[\r\n]/gm, '')
    if (debug) {
      this.logger.info(`\nExecuting:\n  Query: ${query}\n  Values:\n ${values}\n'`)
    }

    if (!this.client) {
      return await this.transaction(async () => this._query(query, values, debug))
    }

    const start = Date.now()
    return await new Promise<QueryResult<T>>((resolve, reject) => {
      this.client!.query<T>(query, values, (err, res) => {
        if (err) {
          if (err.message.includes('user_auth_email_key')) {
            this.logger.error(`Error inserting data with duplicated email: ${JSON.stringify(values)}`)
          } else {
            const errorId = Math.random().toString().substr(2)
            console.error(`Error ${errorId} running statement:`, query, 'with values', JSON.stringify(values))
            this.logger.error(`Error running sql statement ${errorId} ${err.message}`, { errorId })
          }
          reject(err)
          return
        }

        if (debug) {
          const duration = Date.now() - start
          this.logger.info(
            `executed query ${JSON.stringify({
              query,
              duration,
              rows: res.rowCount,
            })}`,
          )
        }
        resolve(res)
      })
    })
  }

  public createFields<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias?: string) {
    const r = fields.reduce((r, field) => {
      r.push(`'${field}'`)
      r.push(`"${alias}".${snakeCase(field as string)}`)
      return r
    }, [] as string[])
    return r.join(',')
  }

  private selectFields<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias?: string) {
    const r = fields.reduce((r, field) => {
      r.push(`"${alias}".${snakeCase(field as string)}`)
      return r
    }, [] as string[])
    return r.join(',')
  }

  public async transaction<T>(fn: (() => Promise<T>)): Promise<T> {
    if (this.client) {
      return await fn()
    }
    this.client = await this.pool.getClient()
    try {
      await this.query('BEGIN;')
      if (this.userId) {
        await this.query(`SET SESSION "session.user_id" = '${this.userId}'`)
      }
      const result = await fn()
      await this.query('COMMIT;')
      return result;
    } catch (e) {
      await this.query('ROLLBACK')
      throw e
    } finally {
      this.client?.release()
      this.client = null
    }
  }
}
