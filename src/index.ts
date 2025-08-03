import { EODHistoricalService } from "./services/eodHistorical.ts";
import { StockDuckDBStorage } from "./storage/stocks.ts";
import { updateConfig } from "./config.ts";
import type { Config } from "./types/index.ts";

export class Stocker {
  public storage: StockDuckDBStorage;
  private dataService: EODHistoricalService;

  constructor(config?: Partial<Config>) {
    if (config) {
      updateConfig(config);
    }

    const dataDir = config?.storage?.dataDir || "./data";
    this.storage = new StockDuckDBStorage(dataDir);
    this.dataService = new EODHistoricalService(config?.sources?.eodhd?.apiKey);
  }

  async init(): Promise<void> {
    await this.storage.init();
  }

  async fetch(
    ticker: string,
    options?: {
      start?: string;
      end?: string;
      update?: boolean;
      fillGaps?: boolean;
    }
  ): Promise<void> {
    const symbol = ticker.toUpperCase();

    // When update is true and no explicit start/end dates are provided,
    // fetch all data to ensure gaps are filled
    if (options?.update && (await this.storage.exists(symbol))) {
      // Only set start date if not explicitly provided
      if (!options.start && !options.end) {
        // Fetch all data to fill any gaps
        console.log(
          `Fetching all data for ${symbol} to fill any gaps...`
        );
      } else if (!options.start) {
        // If only end date is provided, still fetch from beginning
        console.log(
          `Fetching ${symbol} from beginning to ${options.end}...`
        );
      } else {
        // Use provided dates
        console.log(
          `Fetching ${symbol} from ${options.start} to ${
            options.end || "today"
          }...`
        );
      }
    } else {
      console.log(
        `Fetching ${symbol} from ${options?.start || "beginning"} to ${
          options?.end || "today"
        }...`
      );
    }

    // Fetch data
    const data = await this.dataService.fetchDaily(
      symbol,
      options?.start,
      options?.end
    );

    if (data.length === 0) {
      console.log(`No new data for ${symbol}`);
      return;
    }

    // Fetch splits
    const splits = await this.dataService.fetchSplits(symbol);

    // Apply split information
    if (splits.length > 0) {
      const splitMap = new Map(splits.map((s) => [s.date, s]));
      data.forEach((bar) => {
        if (splitMap.has(bar.date)) {
          bar.split = true;
        }
      });
    }

    // Store data
    if (options?.update && (await this.storage.exists(symbol))) {
      await this.storage.mergeDaily(symbol, data);
      console.log(`Updated ${symbol} with ${data.length} new records`);
    } else {
      await this.storage.writeDaily(symbol, data);
      console.log(`Stored ${symbol} with ${data.length} records`);
    }
  }

  async updateSmart(ticker: string): Promise<void> {
    const symbol = ticker.toUpperCase();
    
    if (!(await this.storage.exists(symbol))) {
      console.log(`No existing data for ${symbol}, performing initial fetch...`);
      await this.fetch(ticker);
      return;
    }

    const data = await this.storage.getDaily(symbol);
    
    // Check for gaps
    const gaps = this.findGaps(data);
    
    if (gaps.length > 0) {
      console.log(`Found ${gaps.length} gaps in ${symbol} data`);
      
      // Fetch all data to fill gaps
      await this.fetch(ticker, { update: true });
    } else {
      // Just update from last date
      if (data.length > 0) {
        const lastDate = data[data.length - 1]!.date;
        const nextDate = new Date(lastDate);
        nextDate.setDate(nextDate.getDate() + 1);
        
        await this.fetch(ticker, {
          start: nextDate.toISOString().split("T")[0],
          update: true
        });
      }
    }
  }

  async list(): Promise<string[]> {
    return this.storage.listTickers();
  }

  async info(ticker: string): Promise<void> {
    const symbol = ticker.toUpperCase();
    const exists = await this.storage.exists(symbol);

    if (!exists) {
      console.log(`No data for ${symbol}`);
      return;
    }

    const data = await this.storage.getDaily(symbol);
    const lastUpdate = await this.storage.getLastUpdate(symbol);

    console.log(`Ticker: ${symbol}`);
    console.log(`Records: ${data.length}`);
    console.log(`First date: ${data[0]?.date}`);
    console.log(`Last date: ${data[data.length - 1]?.date}`);
    console.log(`Last update: ${lastUpdate?.toISOString()}`);

    // Check for gaps
    const gaps = this.findGaps(data);
    if (gaps.length > 0) {
      console.log(`\nData gaps detected (${gaps.length}):`);
      gaps.slice(0, 5).forEach(gap => {
        console.log(`  ${gap.start} to ${gap.end} (${gap.days} days)`);
      });
      if (gaps.length > 5) {
        console.log(`  ... and ${gaps.length - 5} more gaps`);
      }
    }
  }

  private findGaps(data: Array<{ date: string }>): Array<{ start: string; end: string; days: number }> {
    const gaps: Array<{ start: string; end: string; days: number }> = [];
    
    if (data.length < 2) return gaps;

    for (let i = 1; i < data.length; i++) {
      const prevDate = new Date(data[i - 1]!.date);
      const currDate = new Date(data[i]!.date);
      
      // Calculate expected next trading day (skip weekends)
      let expectedDate = new Date(prevDate);
      expectedDate.setDate(expectedDate.getDate() + 1);
      
      // Skip weekends
      while (expectedDate.getDay() === 0 || expectedDate.getDay() === 6) {
        expectedDate.setDate(expectedDate.getDate() + 1);
      }
      
      // If there's a gap of more than 1 trading day
      const daysDiff = Math.floor((currDate.getTime() - expectedDate.getTime()) / (1000 * 60 * 60 * 24));
      if (daysDiff > 0) {
        gaps.push({
          start: expectedDate.toISOString().split('T')[0]!,
          end: new Date(currDate.getTime() - 24 * 60 * 60 * 1000).toISOString().split('T')[0]!,
          days: daysDiff + 1
        });
      }
    }
    
    return gaps;
  }

  async query(sql: string): Promise<any[]> {
    // This would be implemented when we have direct DuckDB query access
    throw new Error("Direct SQL queries not yet implemented");
  }

  async close(): Promise<void> {
    await this.storage.close();
  }
}

// Export everything for library usage
export * from "./types/index.ts";
export * from "./config.ts";
export * from "./services/eodHistorical.ts";
export * from "./storage/base.ts";
export * from "./storage/stocks.ts";
