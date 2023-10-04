interface DbConfig {
  defaultDatabase?: string
  user: string
  password: string
  host: string
  port: number
}

interface DbConfigWithName {
  database: string
  user: string
  password: string
  host: string
  port: number
}

interface Config {
  logger: (message: string) => void
}

export function createDb(dbName: string, dbConfig: DbConfig, config: Config): Promise<void>

export function migrate(dbConfig: DbConfigWithName, migrationsDirectory: string, config: Config): Promise<void>