import { FilterSubExpressions } from '@vramework/generic/dist/filter'
import * as pg from 'pg'
import { Pool } from 'pg'
import { exactlyOneResult, getFilters, Logger, sanitizeResult, selectFields, ValueTypes } from './database-utils'

export class TypedPostgresPool<Tables extends { [key: string]: any }, CustomTypes = never> {
  public pool: Pool
  public client!: pg.PoolClient

  constructor(private dbCredentials: pg.PoolConfig, private logger: Logger) {
    this.logger.info(`Using db host: ${dbCredentials.host}`)
    this.pool = new Pool(dbCredentials)
  }

  public async init() {
    this.client = await this.pool.connect()
    await this.checkConnection()
    await this.client.release()
  }

  public async getClient() {
    return this.pool.connect()
  }

  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<Record<keyof T, ValueTypes>> | FilterSubExpressions): Promise<T[]>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<Record<keyof T, ValueTypes>> | FilterSubExpressions, notSingleError: Error): Promise<T>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<Record<keyof T, ValueTypes>> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<T | T[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<T>(`SELECT * FROM "app"."${table}" ${filter}`, filterValues)
    if (notSingleError) {
      return sanitizeResult(exactlyOneResult(result.rows, notSingleError))
    }
    return result.rows
  }

  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<Record<keyof T, ValueTypes>> | FilterSubExpressions): Promise<Pick<T, typeof fields[number]>[]>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<Record<keyof T, ValueTypes>> | FilterSubExpressions, notSingleError: Error): Promise<Pick<T, typeof fields[number]>>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<Record<keyof T, ValueTypes>> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<Pick<T, typeof fields[number]> | Pick<T, typeof fields[number]>[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<Pick<T, typeof fields[number]>>(`
      SELECT ${selectFields<T>(fields, table as string)}
      FROM "app"."${table}"
      ${filter}
    `, filterValues)
    if (notSingleError) {
      return sanitizeResult(exactlyOneResult(result.rows, notSingleError))
    }
    return result.rows
  }

  public async query<T>(statement: string, values?: any[]) {
    return await this.pool.query<T>(statement, values)
  }

  public async close() {
    this.pool.end()
  }

  private async checkConnection(): Promise<void> {
    try {
      const { rows } = await this.client.query<{ serverVersion: string }>('SHOW server_version;')
      this.logger.info(`Postgres server version is: ${rows[0].serverVersion}`)
    } catch (e) {
      console.error(e)
      this.logger.error(`Unable to connect to server with ${this.dbCredentials.host}, exiting server`)
      process.exit(1)
    }
  }

}