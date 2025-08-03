import { join } from 'path';
import { existsSync } from 'fs';
import { DuckDBBase } from './base.ts';
import type { OHLCV, StockMetadata } from '../types/index.ts';

export interface StockStorage {
  getDaily(ticker: string, startDate?: Date, endDate?: Date): Promise<OHLCV[]>;
  exists(ticker: string): Promise<boolean>;
  listTickers(): Promise<string[]>;
  writeDaily(ticker: string, data: OHLCV[]): Promise<void>;
  mergeDaily(ticker: string, newData: OHLCV[]): Promise<void>;
  getMetadata(ticker: string): Promise<StockMetadata | null>;
}

export class StockDuckDBStorage extends DuckDBBase implements StockStorage {
  private stocksDir: string;
  
  constructor(dataDir: string) {
    super(dataDir);
    this.stocksDir = join(dataDir, 'stocks');
  }
  
  override async init(): Promise<void> {
    await super.init();
    
    // Create stocks-specific views
    await this.createViews();
    
    // Create metadata table
    await this.execute(`
      CREATE TABLE IF NOT EXISTS stock_metadata (
        ticker VARCHAR PRIMARY KEY,
        first_trade_date DATE,
        last_trade_date DATE,
        last_update TIMESTAMP,
        data_source VARCHAR,
        record_count INTEGER
      )
    `);
  }
  
  private async createViews(): Promise<void> {
    // Check if any parquet files exist
    const pattern = `${this.stocksDir}/*/daily.parquet`;
    const testQuery = `SELECT COUNT(*) as count FROM glob('${pattern}')`;
    const result = await this.queryOne<{count: number}>(testQuery);
    
    if (result && result.count > 0) {
      // Create view only if files exist
      await this.execute(`
        CREATE OR REPLACE VIEW daily AS
        SELECT 
          regexp_extract(filename, '([^/]+)/daily\\.parquet$', 1) as ticker,
          date,
          -- Raw prices
          open,
          high,
          low,
          close,
          volume,
          -- Adjusted prices
          adj_open,
          adj_high,
          adj_low,
          adj_close,
          adj_volume,
          -- Metadata
          percent_change,
          split,
          data_source,
          fetched_at
        FROM read_parquet('${pattern}', filename=true)
      `);
    } else {
      // Create empty view
      await this.execute(`
        CREATE OR REPLACE VIEW daily AS
        SELECT 
          NULL::VARCHAR as ticker,
          NULL::DATE as date,
          NULL::DOUBLE as open,
          NULL::DOUBLE as high,
          NULL::DOUBLE as low,
          NULL::DOUBLE as close,
          NULL::DOUBLE as adjustedClose,
          NULL::BIGINT as volume,
          NULL::DOUBLE as percentChange
        WHERE 1=0
      `);
    }
  }
  
  async getDaily(ticker: string, startDate?: Date, endDate?: Date): Promise<OHLCV[]> {
    const parquetPath = this.getParquetPath(ticker);
    if (!existsSync(parquetPath)) {
      return [];
    }
    
    let query = `SELECT * FROM read_parquet('${parquetPath}')`;
    const conditions: string[] = [];
    
    if (startDate) {
      conditions.push(`date >= '${startDate.toISOString().split('T')[0]}'`);
    }
    if (endDate) {
      conditions.push(`date <= '${endDate.toISOString().split('T')[0]}'`);
    }
    
    if (conditions.length > 0) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }
    
    query += ' ORDER BY date';
    
    const result = await this.query(query);
    return result.map(row => ({
      date: row.date,
      // Raw prices
      open: Number(row.open),
      high: Number(row.high),
      low: Number(row.low),
      close: Number(row.close),
      volume: Number(row.volume),
      // Adjusted prices
      adjOpen: Number(row.adj_open),
      adjHigh: Number(row.adj_high),
      adjLow: Number(row.adj_low),
      adjClose: Number(row.adj_close),
      adjVolume: Number(row.adj_volume),
      // Metadata
      percentChange: row.percent_change || 0,
      split: row.split || false,
      dataSource: row.data_source,
      fetchedAt: row.fetched_at,
      // Legacy field
      adjustedClose: Number(row.adj_close),
    }));
  }
  
  async exists(ticker: string): Promise<boolean> {
    return existsSync(this.getParquetPath(ticker));
  }
  
  async listTickers(): Promise<string[]> {
    // Check if any files exist first
    const pattern = `${this.stocksDir}/*/daily.parquet`;
    const testQuery = `SELECT COUNT(*) as count FROM glob('${pattern}')`;
    const countResult = await this.queryOne<{count: number}>(testQuery);
    
    if (!countResult || countResult.count === 0) {
      return [];
    }
    
    // If files exist, query them
    const result = await this.query<{file: string}>(`
      SELECT file FROM glob('${pattern}')
    `);
    
    // Extract tickers from file paths
    const tickers = result
      .map(row => {
        const match = row.file.match(/([^/]+)\/daily\.parquet$/);
        return match ? match[1] : null;
      })
      .filter(Boolean) as string[];
    
    return [...new Set(tickers)].sort();
  }
  
  async writeDaily(ticker: string, data: OHLCV[]): Promise<void> {
    if (data.length === 0) return;
    
    const parquetPath = this.getParquetPath(ticker);
    
    // Transform data for storage
    const storageData = data.map(d => ({
      date: d.date,
      // Raw prices
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      volume: d.volume,
      // Adjusted prices
      adj_open: d.adjOpen,
      adj_high: d.adjHigh,
      adj_low: d.adjLow,
      adj_close: d.adjClose,
      adj_volume: d.adjVolume,
      // Metadata
      percent_change: d.percentChange || 0,
      split: d.split || false,
      data_source: d.dataSource || 'eod',
      fetched_at: d.fetchedAt || new Date().toISOString(),
    }));
    
    // Define schema
    const schema = {
      date: 'DATE',
      // Raw prices
      open: 'DOUBLE',
      high: 'DOUBLE',
      low: 'DOUBLE',
      close: 'DOUBLE',
      volume: 'BIGINT',
      // Adjusted prices
      adj_open: 'DOUBLE',
      adj_high: 'DOUBLE',
      adj_low: 'DOUBLE',
      adj_close: 'DOUBLE',
      adj_volume: 'BIGINT',
      // Metadata
      percent_change: 'DOUBLE',
      split: 'BOOLEAN',
      data_source: 'VARCHAR',
      fetched_at: 'TIMESTAMP'
    };
    
    await this.writeParquet(storageData, parquetPath, schema);
    
    // Update metadata
    await this.updateMetadata(ticker, data);
    
    // Recreate views after first write
    await this.createViews();
  }
  
  async mergeDaily(ticker: string, newData: OHLCV[]): Promise<void> {
    if (newData.length === 0) return;
    
    const parquetPath = this.getParquetPath(ticker);
    
    // Transform data for storage
    const storageData = newData.map(d => ({
      date: d.date,
      // Raw prices
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      volume: d.volume,
      // Adjusted prices
      adj_open: d.adjOpen,
      adj_high: d.adjHigh,
      adj_low: d.adjLow,
      adj_close: d.adjClose,
      adj_volume: d.adjVolume,
      // Metadata
      percent_change: d.percentChange || 0,
      split: d.split || false,
      data_source: d.dataSource || 'eod',
      fetched_at: d.fetchedAt || new Date().toISOString(),
    }));
    
    await this.mergeParquet(parquetPath, storageData, 'date');
    
    // Update metadata
    await this.updateMetadata(ticker, newData);
  }
  
  async getMetadata(ticker: string): Promise<StockMetadata | null> {
    const result = await this.queryOne<any>(`
      SELECT * FROM stock_metadata WHERE ticker = ?
    `, ticker);
    
    if (!result) return null;
    
    return {
      ticker: result.ticker,
      firstTradeDate: result.first_trade_date,
      lastUpdate: result.last_update,
      dataSource: result.data_source,
      recordCount: result.record_count,
    };
  }
  
  private async updateMetadata(ticker: string, data: OHLCV[]): Promise<void> {
    if (data.length === 0) return;
    
    const firstDate = data.reduce((min, d) => d.date < min ? d.date : min, data[0]!.date);
    const lastDate = data.reduce((max, d) => d.date > max ? d.date : max, data[0]!.date);
    
    // Get record count from parquet file
    const countResult = await this.queryOne<{count: number}>(`
      SELECT COUNT(*) as count FROM read_parquet('${this.getParquetPath(ticker)}')
    `);
    
    await this.execute(`
      INSERT OR REPLACE INTO stock_metadata 
      (ticker, first_trade_date, last_trade_date, last_update, data_source, record_count)
      VALUES (?, ?, ?, ?, ?, ?)
    `, ticker, firstDate, lastDate, new Date().toISOString(), 'eod', countResult?.count || 0);
  }
  
  private getParquetPath(ticker: string): string {
    return join(this.stocksDir, ticker, 'daily.parquet');
  }
  
  async getLastUpdate(ticker: string): Promise<Date | null> {
    const metadata = await this.getMetadata(ticker);
    return metadata ? new Date(metadata.lastUpdate) : null;
  }
}