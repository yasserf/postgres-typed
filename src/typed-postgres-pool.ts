import { FilterSubExpressions } from '@vramework/generic/dist/filter'
import * as pg from 'pg'
import { Pool } from 'pg'
import { exactlyOneResult, getFilters, Logger, QueryInterface, sanitizeResult, ValueTypes } from './database-utils'
import { snakeCase } from 'snake-case'
import { TypedPostgresClient } from './typed-postgres-client'

export class TypedPostgresPool<Tables extends { [key: string]: any }, CustomTypes = never> {
  public pool: Pool
  public client!: pg.PoolClient

  constructor(dbCredentialsOrPool: pg.PoolConfig | Pool, private logger: Logger, public schema: string) {
    if (dbCredentialsOrPool instanceof Pool) {
      this.pool = dbCredentialsOrPool
    } else {
      this.logger.info(`Using db host: ${dbCredentialsOrPool.host}`)
      this.pool = new Pool(dbCredentialsOrPool)
    }

  }

  public async init() {
    this.client = await this.pool.connect()
    await this.checkConnection()
    await this.client.release()
  }

  public async getClient() {
    return this.pool.connect()
  }

  public getTypedClient(userId: string): TypedPostgresClient<Tables, CustomTypes> {
    return new TypedPostgresClient<Tables, CustomTypes>(this, this.logger, userId)
  }

  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions): Promise<T[]>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions, notSingleError: Error): Promise<T>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<T | T[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<T>(`SELECT * FROM "${this.schema}"."${String(table)}" ${filter}`, filterValues)
    if (notSingleError) {
      return sanitizeResult(exactlyOneResult(result.rows, notSingleError))
    }
    return result.rows
  }

  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions): Promise<Pick<T, typeof fields[number]>[]>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions, notSingleError: Error): Promise<Pick<T, typeof fields[number]>>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<Pick<T, typeof fields[number]> | Pick<T, typeof fields[number]>[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<Pick<T, typeof fields[number]>>(({ sf }) => `
      SELECT ${sf(table, fields)}
      FROM "${this.schema}"."${String(table)}"
      ${filter}
    `, filterValues)
    if (notSingleError) {
      return sanitizeResult(exactlyOneResult(result.rows, notSingleError))
    }
    return result.rows
  }

  public async one<T>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = [],
    error: Error
  ): Promise<T> {
    const r = await this.query<T>(statement, values)
    return exactlyOneResult(r.rows, error)
  }

  public async many<T>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = []
  ): Promise<T[]> {
    const r = await this.query<T>(statement, values)
    return r.rows
  }

  public async query<T>(statement: QueryInterface<Tables>, values?: any[]) {
    const query = typeof statement === 'string' ? statement : statement({
      cf: this.createFields,
      sf: this.selectFields
    })
    return await this.pool.query<T>(query, values)
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
      this.logger.error(`Unable to connect to server, exiting server`)
      process.exit(1)
    }
  }

  public createFields<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias: string = table as string) {
    const r = fields.reduce((r, field) => {
      r.push(`'${String(field)}'`)
      r.push(`"${alias}".${snakeCase(field as string)}`)
      return r
    }, [] as string[])
    return r.join(',')
  }

  private selectFields<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias: string = table as string) {
    const r = fields.reduce((r, field) => {
      r.push(`"${alias}".${snakeCase(field as string)}`)
      return r
    }, [] as string[])
    return r.join(',')
  }

}