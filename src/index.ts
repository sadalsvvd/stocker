import { EODHistoricalService } from "./services/eodHistorical.ts";
import { StockDuckDBStorage } from "./storage/stocks.ts";
import { ReferenceStorage } from "./storage/reference.ts";
import { updateConfig } from "./config.ts";
import type { Config, TickerInfo } from "./types/index.ts";

export class Stocker {
  public storage: StockDuckDBStorage;
  private dataService: EODHistoricalService;
  private referenceStorage: ReferenceStorage;

  constructor(config?: Partial<Config>) {
    if (config) {
      updateConfig(config);
    }

    const dataDir = config?.storage?.dataDir || "./data";
    this.storage = new StockDuckDBStorage(dataDir);
    this.dataService = new EODHistoricalService(config?.sources?.eodhd?.apiKey);
    this.referenceStorage = new ReferenceStorage(dataDir);
  }

  async init(): Promise<void> {
    await this.storage.init();
    await this.referenceStorage.init();
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
        console.log(`Fetching all data for ${symbol} to fill any gaps...`);
      } else if (!options.start) {
        // If only end date is provided, still fetch from beginning
        console.log(`Fetching ${symbol} from beginning to ${options.end}...`);
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

    // Track ticker in registry if not already present
    const existingTicker = await this.referenceStorage.getTicker(symbol);
    if (!existingTicker) {
      console.log(`Adding ${symbol} to ticker registry...`);
      const tickerInfo: TickerInfo = {
        symbol: symbol,
        companyName: symbol, // We don't have company name from EOD data alone
        exchange: "US",
        status: "active",
        firstSeen: new Date().toISOString().split("T")[0]!,
        lastUpdated: new Date().toISOString().split("T")[0]!,
        metadata: {
          source: "manual_fetch",
          addedBy: "stocker_fetch",
        },
      };
      await this.referenceStorage.upsertTickers([tickerInfo]);
    }
  }

  async updateSmart(ticker: string): Promise<void> {
    const symbol = ticker.toUpperCase();

    if (!(await this.storage.exists(symbol))) {
      console.log(
        `No existing data for ${symbol}, performing initial fetch...`
      );
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
          update: true,
        });
      }
    }
  }

  async list(): Promise<string[]> {
    return this.storage.listTickers();
  }

  async info(
    ticker: string,
    options?: { showAllGaps?: boolean }
  ): Promise<void> {
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

    if (options?.showAllGaps && gaps.length > 0) {
      // Show all gaps
      console.log(`\nAll data gaps detected (${gaps.length}):`);
      console.log(
        "Note: Includes market holidays and any missing trading days"
      );
      gaps.slice(0, 10).forEach((gap) => {
        console.log(`  ${gap.start} to ${gap.end} (${gap.days} trading days)`);
      });
      if (gaps.length > 10) {
        console.log(`  ... and ${gaps.length - 10} more gaps`);
      }
    } else {
      // Show only significant gaps (>10 trading days)
      const significantGaps = gaps.filter((gap) => gap.days > 10);

      if (significantGaps.length > 0) {
        console.log(
          `\nSignificant data gaps detected (${significantGaps.length} gaps > 10 trading days):`
        );
        significantGaps.slice(0, 5).forEach((gap) => {
          console.log(
            `  ${gap.start} to ${gap.end} (${gap.days} trading days)`
          );
        });
        if (significantGaps.length > 5) {
          console.log(
            `  ... and ${significantGaps.length - 5} more significant gaps`
          );
        }
        console.log(
          `\nTotal gaps: ${gaps.length} (including holidays and minor gaps)`
        );
      } else if (gaps.length > 0) {
        console.log(
          `\nNo significant gaps found (${gaps.length} minor gaps including holidays)`
        );
      }
    }
  }

  private findGaps(
    data: Array<{ date: string }>
  ): Array<{ start: string; end: string; days: number }> {
    const gaps: Array<{ start: string; end: string; days: number }> = [];

    if (data.length < 2) return gaps;

    // Helper to check if a date is a trading day (weekday)
    const isTradingDay = (date: Date): boolean => {
      const day = date.getDay();
      return day !== 0 && day !== 6; // Not Sunday (0) or Saturday (6)
    };

    // Helper to get next trading day
    const getNextTradingDay = (date: Date): Date => {
      const next = new Date(date);
      next.setDate(next.getDate() + 1);

      while (!isTradingDay(next)) {
        next.setDate(next.getDate() + 1);
      }

      return next;
    };

    // Helper to count trading days between two dates (exclusive)
    const countTradingDaysBetween = (start: Date, end: Date): number => {
      let count = 0;
      const current = new Date(start);
      current.setDate(current.getDate() + 1); // Start from day after start

      while (current < end) {
        if (isTradingDay(current)) {
          count++;
        }
        current.setDate(current.getDate() + 1);
      }

      return count;
    };

    for (let i = 1; i < data.length; i++) {
      const prevDate = new Date(data[i - 1]!.date);
      const currDate = new Date(data[i]!.date);

      // Count missing trading days between previous and current
      const missingTradingDays = countTradingDaysBetween(prevDate, currDate);

      // Only report if there are missing trading days
      if (missingTradingDays > 0) {
        // Find the actual start and end dates of the gap
        const gapStart = getNextTradingDay(prevDate);
        const gapEnd = new Date(currDate);
        gapEnd.setDate(gapEnd.getDate() - 1);

        // Skip weekends at the end of the gap
        while (!isTradingDay(gapEnd) && gapEnd > gapStart) {
          gapEnd.setDate(gapEnd.getDate() - 1);
        }

        gaps.push({
          start: gapStart.toISOString().split("T")[0]!,
          end: gapEnd.toISOString().split("T")[0]!,
          days: missingTradingDays,
        });
      }
    }

    return gaps;
  }

  async query(sql: string): Promise<any[]> {
    // This would be implemented when we have direct DuckDB query access
    throw new Error("Direct SQL queries not yet implemented");
  }
}

// Export everything for library usage
export * from "./types/index.ts";
export type { OHLCV } from "./types/index.ts";
export * from "./config.ts";
export * from "./services/eodHistorical.ts";
export * from "./services/sec.ts";
export * from "./services/price-loader.ts";
export * from "./storage/base.ts";
export * from "./storage/stocks.ts";
export * from "./storage/reference.ts";
