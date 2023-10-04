/* eslint-disable */

export type ConnectionConfig = {
  database: string
  user: string
  password: string
  host: string
  port: string
  defaultDatabase?: string
}

export type CreateConfig  = {
  logger(message: string): void
}

export type MigrateConfig = {
  logger(message: string): void
  numberMigrationsToLoad: number
}

export default interface PostgresMigrations {
  createDb(dbName: string, dbConfig: ConnectionConfig, config: CreateConfig): Promise<void>
  migrate(dbConfig: ConnectionConfig, migrationsDirectory: string, config: MigrateConfig): Promoise<void>
}
