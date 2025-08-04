import { DuckDBBase } from "../storage/base.js";
import { join } from "path";
import { existsSync } from "fs";
import type { OHLCV } from "../types/index.js";

export class PriceLoader extends DuckDBBase {
  private stocksDir: string;

  constructor(dataDir: string) {
    super(dataDir);
    this.stocksDir = join(dataDir, "stocks");
  }

  override async init(): Promise<void> {
    await super.init();
    // No need to create tables, we'll read directly from parquet files
  }

  /**
   * Load all price data for a given symbol
   */
  async loadPrices(symbol: string): Promise<OHLCV[]> {
    const parquetPath = join(this.stocksDir, symbol, "daily.parquet");

    if (!existsSync(parquetPath)) {
      return [];
    }

    try {
      const results = await this.query<any>(`
        SELECT 
          date,
          open,
          high,
          low,
          close,
          volume,
          adj_open,
          adj_high,
          adj_low,
          adj_close,
          adj_volume,
          percent_change,
          split,
          data_source,
          fetched_at
        FROM read_parquet('${parquetPath}')
        ORDER BY date ASC
      `);

      return results.map((row) => ({
        date: row.date,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: Number(row.volume),
        adjOpen: row.adj_open || row.open,
        adjHigh: row.adj_high || row.high,
        adjLow: row.adj_low || row.low,
        adjClose: row.adj_close || row.close,
        adjVolume: Number(row.adj_volume || row.volume),
        percentChange: row.percent_change,
        split: row.split,
        dividends: undefined, // Not in parquet
        dataSource: row.data_source,
        fetchedAt: row.fetched_at,
      }));
    } catch (error) {
      console.error(`Error loading prices for ${symbol}:`, error);
      return [];
    }
  }

  /**
   * Load price data for a specific date range
   */
  async loadPricesRange(
    symbol: string,
    startDate: string,
    endDate: string
  ): Promise<OHLCV[]> {
    const parquetPath = join(this.stocksDir, symbol, "daily.parquet");

    if (!existsSync(parquetPath)) {
      return [];
    }

    try {
      const results = await this.query<any>(`
        SELECT 
          date,
          open,
          high,
          low,
          close,
          volume,
          adj_open,
          adj_high,
          adj_low,
          adj_close,
          adj_volume,
          percent_change,
          split,
          data_source,
          fetched_at
        FROM read_parquet('${parquetPath}')
        WHERE date >= '${startDate}' AND date <= '${endDate}'
        ORDER BY date ASC
      `);

      return results.map((row) => ({
        date: row.date,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: Number(row.volume),
        adjOpen: row.adj_open || row.open,
        adjHigh: row.adj_high || row.high,
        adjLow: row.adj_low || row.low,
        adjClose: row.adj_close || row.close,
        adjVolume: Number(row.adj_volume || row.volume),
        percentChange: row.percent_change,
        split: row.split,
        dividends: undefined, // Not in parquet
        dataSource: row.data_source,
        fetchedAt: row.fetched_at,
      }));
    } catch (error) {
      console.error(`Error loading prices for ${symbol}:`, error);
      return [];
    }
  }

  /**
   * Get latest price for a symbol
   */
  async getLatestPrice(symbol: string): Promise<OHLCV | null> {
    const parquetPath = join(this.stocksDir, symbol, "daily.parquet");

    if (!existsSync(parquetPath)) {
      return null;
    }

    try {
      const result = await this.queryOne<any>(`
        SELECT 
          date,
          open,
          high,
          low,
          close,
          volume,
          adj_open,
          adj_high,
          adj_low,
          adj_close,
          adj_volume,
          percent_change,
          split,
          data_source,
          fetched_at
        FROM read_parquet('${parquetPath}')
        ORDER BY date DESC
        LIMIT 1
      `);

      if (!result) return null;

      return {
        date: result.date,
        open: result.open,
        high: result.high,
        low: result.low,
        close: result.close,
        volume: result.volume,
        adjOpen: result.adjOpen,
        adjHigh: result.adjHigh,
        adjLow: result.adjLow,
        adjClose: result.adjClose,
        adjVolume: result.adjVolume,
        percentChange: result.percentChange,
        split: result.split,
        dividends: result.dividends,
        dataSource: result.dataSource,
        fetchedAt: result.fetchedAt,
      };
    } catch (error) {
      console.error(`Error loading latest price for ${symbol}:`, error);
      return null;
    }
  }

  /**
   * Get first available price for a symbol
   */
  async getFirstPrice(symbol: string): Promise<OHLCV | null> {
    const parquetPath = join(this.stocksDir, symbol, "daily.parquet");

    if (!existsSync(parquetPath)) {
      return null;
    }

    try {
      const result = await this.queryOne<any>(`
        SELECT 
          date,
          open,
          high,
          low,
          close,
          volume,
          adj_open,
          adj_high,
          adj_low,
          adj_close,
          adj_volume,
          percent_change,
          split,
          data_source,
          fetched_at
        FROM read_parquet('${parquetPath}')
        ORDER BY date ASC
        LIMIT 1
      `);

      if (!result) return null;

      return {
        date: result.date,
        open: result.open,
        high: result.high,
        low: result.low,
        close: result.close,
        volume: result.volume,
        adjOpen: result.adjOpen,
        adjHigh: result.adjHigh,
        adjLow: result.adjLow,
        adjClose: result.adjClose,
        adjVolume: result.adjVolume,
        percentChange: result.percentChange,
        split: result.split,
        dividends: result.dividends,
        dataSource: result.dataSource,
        fetchedAt: result.fetchedAt,
      };
    } catch (error) {
      console.error(`Error loading first price for ${symbol}:`, error);
      return null;
    }
  }

  /**
   * Check if price data exists for a symbol
   */
  hasPriceData(symbol: string): boolean {
    const parquetPath = join(this.stocksDir, symbol, "daily.parquet");
    return existsSync(parquetPath);
  }

  /**
   * Get available symbols with price data
   */
  async getAvailableSymbols(): Promise<string[]> {
    try {
      const { readdirSync } = await import("fs");
      const symbols = readdirSync(this.stocksDir);

      // Filter to only include directories that contain daily.parquet
      return symbols
        .filter((symbol) => {
          const parquetPath = join(this.stocksDir, symbol, "daily.parquet");
          return existsSync(parquetPath);
        })
        .sort();
    } catch {
      return [];
    }
  }

  async close(): Promise<void> {
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }
}

// Singleton instance for convenience
let priceLoader: PriceLoader | null = null;

export function getPriceLoader(dataDir?: string): PriceLoader {
  if (!priceLoader) {
    const dir = dataDir || process.env.STOCKER_DATA_DIR || "./data";
    priceLoader = new PriceLoader(dir);
  }
  return priceLoader;
}

// Convenience functions for direct import
export async function loadPrices(symbol: string): Promise<OHLCV[]> {
  const loader = getPriceLoader();
  await loader.init();
  return loader.loadPrices(symbol);
}

export async function getLatestPrice(symbol: string): Promise<OHLCV | null> {
  const loader = getPriceLoader();
  await loader.init();
  return loader.getLatestPrice(symbol);
}

export async function getFirstPrice(symbol: string): Promise<OHLCV | null> {
  const loader = getPriceLoader();
  await loader.init();
  return loader.getFirstPrice(symbol);
}
