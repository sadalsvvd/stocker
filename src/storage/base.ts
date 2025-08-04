import { Database } from "duckdb-async";
import { existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";

export interface ParquetTable {
  name: string;
  path: string;
  schema: Record<string, string>;
}

export class DuckDBBase {
  protected db: Database | null = null;
  protected _dataDir: string;

  constructor(dataDir: string) {
    this._dataDir = dataDir;
  }
  
  get dataDir(): string {
    return this._dataDir;
  }

  async init(): Promise<void> {
    // Ensure data directory exists
    if (!existsSync(this._dataDir)) {
      mkdirSync(this._dataDir, { recursive: true });
    }

    // Initialize DuckDB
    const catalogPath = join(this._dataDir, "catalog.duckdb");
    this.db = await Database.create(catalogPath);
  }

  async query<T = any>(sql: string, ...params: any[]): Promise<T[]> {
    if (!this.db) throw new Error("Database not initialized");
    return this.db.all(sql, ...params) as Promise<T[]>;
  }

  async queryOne<T = any>(sql: string, ...params: any[]): Promise<T | null> {
    if (!this.db) throw new Error("Database not initialized");
    const results = (await this.db.all(sql, ...params)) as T[];
    return results[0] || null;
  }

  async execute(sql: string, ...params: any[]): Promise<void> {
    if (!this.db) throw new Error("Database not initialized");
    await this.db.run(sql, ...params);
  }

  async writeParquet(
    data: any[],
    outputPath: string,
    schema?: Record<string, string>
  ): Promise<void> {
    if (!this.db) throw new Error("Database not initialized");

    // Ensure directory exists
    const dir = dirname(outputPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    // Create temp table
    const tempTable = `temp_write_${Date.now()}`;

    if (schema) {
      const columns = Object.entries(schema)
        .map(([name, type]) => `${name} ${type}`)
        .join(", ");
      await this.execute(`CREATE TEMP TABLE ${tempTable} (${columns})`);
    } else {
      // Infer schema from first row
      if (data.length === 0) return;

      const firstRow = data[0];
      const columns = Object.keys(firstRow)
        .map((key) => {
          const value = firstRow[key];
          let type = "VARCHAR";
          if (typeof value === "number") {
            type = Number.isInteger(value) ? "BIGINT" : "DOUBLE";
          } else if (value instanceof Date) {
            type = "DATE";
          } else if (typeof value === "boolean") {
            type = "BOOLEAN";
          }
          return `${key} ${type}`;
        })
        .join(", ");
      await this.execute(`CREATE TEMP TABLE ${tempTable} (${columns})`);
    }

    // Insert data in batches (DuckDB doesn't support parameterized inserts well)
    const batchSize = 1000;
    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);
      const columns = Object.keys(batch[0]);
      const values = batch
        .map(
          (row) =>
            `(${columns
              .map((col) => {
                const val = row[col];
                if (val === null || val === undefined) return "NULL";
                if (typeof val === "string")
                  return `'${val.replace(/'/g, "''")}'`;
                if (val instanceof Date)
                  return `'${val.toISOString().split("T")[0]}'`;
                if (typeof val === "boolean") return val ? "TRUE" : "FALSE";
                return val;
              })
              .join(", ")})`
        )
        .join(", ");

      await this.execute(`INSERT INTO ${tempTable} VALUES ${values}`);
    }

    // Write to parquet
    await this.execute(`
      COPY ${tempTable} 
      TO '${outputPath}' 
      (FORMAT PARQUET, COMPRESSION 'ZSTD')
    `);

    // Clean up
    await this.execute(`DROP TABLE ${tempTable}`);
  }

  async readParquet<T = any>(path: string, columns?: string[]): Promise<T[]> {
    if (!this.db) throw new Error("Database not initialized");

    if (!existsSync(path)) {
      return [];
    }

    const selectClause = columns ? columns.join(", ") : "*";
    return this.query<T>(`SELECT ${selectClause} FROM read_parquet('${path}')`);
  }

  async mergeParquet(
    existingPath: string,
    newData: any[],
    keyColumn: string,
    outputPath?: string
  ): Promise<void> {
    if (!this.db) throw new Error("Database not initialized");

    const targetPath = outputPath || existingPath;
    const tempPath = `${targetPath}.tmp`;

    // Create temp table with new data
    const tempTable = `merge_data_${Date.now()}`;

    // Get schema from existing file if it exists
    let schema: Record<string, string> | undefined;
    if (existsSync(existingPath)) {
      // Create a temp view to get schema info
      const tempView = `temp_schema_${Date.now()}`;
      await this.execute(`
        CREATE TEMP VIEW ${tempView} AS 
        SELECT * FROM read_parquet('${existingPath}') LIMIT 1
      `);

      const schemaQuery = await this.query<{
        column_name: string;
        column_type: string;
      }>(`DESCRIBE ${tempView}`);

      schema = Object.fromEntries(
        schemaQuery.map((row) => [row.column_name, row.column_type])
      );

      await this.execute(`DROP VIEW ${tempView}`);
    }

    // Write new data to temp table
    await this.writeToTempTable(tempTable, newData, schema);

    if (existsSync(existingPath)) {
      // Merge with existing data
      await this.execute(`
        COPY (
          SELECT * FROM (
            SELECT * FROM read_parquet('${existingPath}')
            WHERE ${keyColumn} NOT IN (SELECT ${keyColumn} FROM ${tempTable})
            UNION ALL
            SELECT * FROM ${tempTable}
          ) 
          ORDER BY ${keyColumn}
        ) TO '${tempPath}' 
        (FORMAT PARQUET, COMPRESSION 'ZSTD')
      `);

      // Atomic replace
      const fs = await import("fs/promises");
      await fs.rename(tempPath, targetPath);
    } else {
      // No existing data, just write new
      await this.execute(`
        COPY ${tempTable} TO '${targetPath}' 
        (FORMAT PARQUET, COMPRESSION 'ZSTD')
      `);
    }

    // Clean up
    await this.execute(`DROP TABLE ${tempTable}`);
  }

  private async writeToTempTable(
    tableName: string,
    data: any[],
    schema?: Record<string, string>
  ): Promise<void> {
    if (data.length === 0) return;

    // Create table with schema
    if (schema) {
      const columns = Object.entries(schema)
        .map(([name, type]) => `${name} ${type}`)
        .join(", ");
      await this.execute(`CREATE TEMP TABLE ${tableName} (${columns})`);
    } else {
      // Infer from data
      const firstRow = data[0];
      const columns = Object.keys(firstRow)
        .map((key) => {
          const value = firstRow[key];
          let type = "VARCHAR";
          if (typeof value === "number") {
            type = Number.isInteger(value) ? "BIGINT" : "DOUBLE";
          } else if (value instanceof Date) {
            type = "DATE";
          } else if (typeof value === "boolean") {
            type = "BOOLEAN";
          }
          return `${key} ${type}`;
        })
        .join(", ");
      await this.execute(`CREATE TEMP TABLE ${tableName} (${columns})`);
    }

    // Insert data
    const batchSize = 1000;
    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);
      const columns = Object.keys(batch[0]);
      const values = batch
        .map(
          (row) =>
            `(${columns
              .map((col) => {
                const val = row[col];
                if (val === null || val === undefined) return "NULL";
                if (typeof val === "string")
                  return `'${val.replace(/'/g, "''")}'`;
                if (val instanceof Date)
                  return `'${val.toISOString().split("T")[0]}'`;
                return val;
              })
              .join(", ")})`
        )
        .join(", ");

      await this.execute(`INSERT INTO ${tableName} VALUES ${values}`);
    }
  }
}
