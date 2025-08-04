import { SECService } from "./sec.js";
import { FinvizFetcher } from "./finviz-fetcher.js";
import { EODHistoricalService } from "./eodHistorical.js";
import { ReferenceStorage, TickerMaster } from "../storage/reference.js";
import type { TickerInfo } from "../types/index.js";

interface UpdateResult {
  updated: number;
  added: number;
  delisted: number;
  errors: string[];
}

export class TickerUpdaterService {
  private sec: SECService;
  private finviz: FinvizFetcher;
  private eod?: EODHistoricalService;
  private storage: ReferenceStorage;

  constructor(storage: ReferenceStorage, eodApiKey?: string) {
    this.storage = storage;
    this.sec = new SECService();
    this.finviz = new FinvizFetcher();
    if (eodApiKey) {
      this.eod = new EODHistoricalService(eodApiKey);
    }
  }

  /**
   * Update ticker universe from all available sources
   */
  async updateTickerUniverse(): Promise<UpdateResult> {
    console.log("Starting ticker universe update...");

    const result: UpdateResult = {
      updated: 0,
      added: 0,
      delisted: 0,
      errors: [],
    };

    try {
      // 1. Get current tickers from storage
      const existingTickers = await this.storage.getAllTickers("active");
      const tickerMap = new Map(existingTickers.map((t) => [t.symbol, t]));

      // 2. Fetch from SEC (official US tickers)
      console.log("Fetching from SEC...");
      const secTickers = await this.sec.fetchAllTickers();

      // 3. Fetch from Finviz (includes more metadata)
      console.log("Fetching ticker symbols from Finviz...");
      let finvizSymbols: string[] = [];
      try {
        finvizSymbols = await this.finviz.getAllTickersFromScreener();
        console.log(`Fetched ${finvizSymbols.length} ticker symbols from Finviz`);
        if (finvizSymbols.length > 0) {
          console.log(`Sample symbols: ${finvizSymbols.slice(0, 10).join(", ")}...`);
        }
      } catch (error) {
        console.log("Finviz fetch failed, continuing with SEC data only");
        result.errors.push(`Finviz fetch failed: ${error}`);
      }

      // 4. Merge data sources
      const mergedTickers = new Map<string, TickerMaster>();

      // Process SEC data first (authoritative for US stocks)
      for (const secTicker of secTickers) {
        const enhanced: TickerMaster = {
          ...secTicker,
          securityType: "stock",
          sourceRating: "A", // SEC data is authoritative
          dataSources: ["SEC"],
          metadata: {
            ...secTicker.metadata,
            dataSources: ["SEC"],
          },
        };
        mergedTickers.set(secTicker.symbol, enhanced);
      }

      // Enhance with Finviz data
      console.log("Processing Finviz symbols for detailed data...");
      let finvizProcessed = 0;
      let finvizEnhanced = 0;
      
      // Process Finviz symbols in batches
      const finvizBatchSize = 50;
      for (let i = 0; i < finvizSymbols.length; i += finvizBatchSize) {
        const batch = finvizSymbols.slice(i, i + finvizBatchSize);
        console.log(`Processing Finviz batch ${Math.floor(i / finvizBatchSize) + 1}/${Math.ceil(finvizSymbols.length / finvizBatchSize)} (${batch.length} symbols)`);
        
        for (const symbol of batch) {
          finvizProcessed++;
          
          // For now, just mark that we've seen this symbol on Finviz
          const existing = mergedTickers.get(symbol);
          if (existing) {
            // Enhance existing ticker with Finviz presence
            if (!existing.metadata) existing.metadata = {};
            existing.metadata.dataSources = [...(existing.metadata.dataSources || []), "Finviz"];
            finvizEnhanced++;
          } else {
            // New ticker from Finviz only
            const enhanced: TickerMaster = {
              symbol: symbol,
              companyName: symbol, // Will be updated later
              exchange: "UNKNOWN",
              status: "active",
              firstSeen: new Date().toISOString(),
              lastUpdated: new Date().toISOString(),
              securityType: "stock",
              sourceRating: "C", // Finviz only, no details
              metadata: {
                dataSources: ["Finviz"],
              },
            };
            mergedTickers.set(symbol, enhanced);
          }
          
          // Log progress every 1000 symbols
          if (finvizProcessed % 1000 === 0) {
            console.log(`Processed ${finvizProcessed}/${finvizSymbols.length} Finviz symbols (${finvizEnhanced} enhanced)`);
          }
        }
      }
      
      console.log(`Finished processing ${finvizProcessed} Finviz symbols (${finvizEnhanced} enhanced)`);
      console.log(`Total merged tickers: ${mergedTickers.size}`);

      // 5. Find delisted tickers
      console.log("Checking for delisted tickers...");
      const currentSymbols = new Set(mergedTickers.keys());
      const delistedTickers: TickerMaster[] = [];

      for (const [symbol, ticker] of tickerMap) {
        if (!currentSymbols.has(symbol)) {
          delistedTickers.push({
            ...ticker,
            status: "delisted",
            delistedDate: new Date().toISOString().split("T")[0],
            lastUpdated: new Date().toISOString(),
            metadata: {
              ...ticker.metadata,
              delistingReason: "Not found in current sources",
            },
          });
          result.delisted++;
        }
      }

      if (delistedTickers.length > 0) {
        console.log(`Found ${delistedTickers.length} delisted tickers`);
        await this.storage.upsertTickers(delistedTickers);
      }

      // 6. Update or add tickers
      console.log(`Processing ${mergedTickers.size} tickers...`);
      const batchSize = 100;
      const tickerArray = Array.from(mergedTickers.entries());

      for (let i = 0; i < tickerArray.length; i += batchSize) {
        const batch = tickerArray.slice(i, i + batchSize);
        const batchTickers: TickerMaster[] = [];

        for (const [symbol, ticker] of batch) {
          try {
            const existing = tickerMap.get(symbol);

            // Get first trade date if missing
            if (!ticker.firstTradeDate && !existing?.metadata?.firstTradeDate) {
              ticker.firstTradeDate = await this.findFirstTradeDate(symbol);
            }

            // Determine exchange location
            const location = this.getExchangeLocation(ticker.exchange);
            ticker.lat = location.lat;
            ticker.lng = location.lng;
            ticker.timezone = location.timezone;

            batchTickers.push(ticker);

            if (existing) {
              result.updated++;
            } else {
              result.added++;
            }
          } catch (error) {
            result.errors.push(`Failed to process ${symbol}: ${error}`);
          }
        }

        // Batch upsert
        if (batchTickers.length > 0) {
          const batchNum = Math.floor(i / batchSize) + 1;
          const totalBatches = Math.ceil(tickerArray.length / batchSize);
          const progress = Math.round(
            ((i + batchTickers.length) / tickerArray.length) * 100
          );
          console.log(
            `[${progress}%] Upserting batch ${batchNum}/${totalBatches} (${batchTickers.length} tickers)`
          );
          await this.storage.upsertTickers(batchTickers);
        }
      }

      console.log(
        `Update complete. Added: ${result.added}, Updated: ${result.updated}, Delisted: ${result.delisted}`
      );
    } catch (error) {
      result.errors.push(`Update failed: ${error}`);
    }

    return result;
  }

  /**
   * Find first trade date from price history
   */
  private async findFirstTradeDate(
    symbol: string
  ): Promise<string | undefined> {
    if (!this.eod) return undefined;

    try {
      // Fetch max history
      const prices = await this.eod.fetchDaily(symbol, "1900-01-01");
      if (prices && prices.length > 0) {
        return prices[0].date;
      }
    } catch (error) {
      // Ignore errors, just return undefined
    }

    return undefined;
  }

  /**
   * Get exchange location coordinates
   */
  private getExchangeLocation(exchange: string): {
    lat: number;
    lng: number;
    timezone: string;
  } {
    const EXCHANGE_LOCATIONS: Record<
      string,
      { lat: number; lng: number; timezone: string }
    > = {
      NYSE: { lat: 40.7069, lng: -74.0113, timezone: "America/New_York" },
      NASDAQ: { lat: 40.7489, lng: -73.968, timezone: "America/New_York" },
      AMEX: { lat: 40.7069, lng: -74.0113, timezone: "America/New_York" },
      LSE: { lat: 51.5155, lng: -0.0922, timezone: "Europe/London" },
      TSE: { lat: 35.6785, lng: 139.7704, timezone: "Asia/Tokyo" },
      HKEX: { lat: 22.2855, lng: 114.1577, timezone: "Asia/Hong_Kong" },
      SSE: { lat: 31.2304, lng: 121.4737, timezone: "Asia/Shanghai" },
    };

    // Handle variations
    const normalized = exchange.toUpperCase().replace(/[^A-Z]/g, "");
    for (const [key, value] of Object.entries(EXCHANGE_LOCATIONS)) {
      if (normalized.includes(key)) {
        return value;
      }
    }

    // Default to NYSE
    return EXCHANGE_LOCATIONS.NYSE || { lat: 40.7069, lng: -74.0113, timezone: "America/New_York" };
  }

  /**
   * Verify and rate ticker data quality
   */
  async verifyAndRateTickers(): Promise<void> {
    console.log("Verifying ticker data quality...");

    const tickers = await this.storage.getAllTickers("active");
    let updated = 0;

    for (const ticker of tickers) {
      try {
        // Calculate source rating based on data completeness
        let rating = "F";
        let score = 0;

        // Check data completeness
        if (ticker.companyName && ticker.companyName !== ticker.symbol)
          score += 20;
        if (ticker.exchange && ticker.exchange !== "UNKNOWN") score += 20;
        if (ticker.sector) score += 10;
        if (ticker.industry) score += 10;
        if (ticker.firstTradeDate || ticker.ipoDate) score += 20;
        if (ticker.metadata?.dataSources?.length > 1) score += 20;

        // Convert score to rating
        if (score >= 90) rating = "A";
        else if (score >= 70) rating = "B";
        else if (score >= 50) rating = "C";
        else if (score >= 30) rating = "D";

        // Update if rating changed
        if (ticker.sourceRating !== rating) {
          await this.storage.upsertTickers([
            {
              ...ticker,
              sourceRating: rating,
              lastUpdated:
                ticker.lastUpdated || new Date().toISOString(),
            },
          ]);
          updated++;
        }
      } catch (error) {
        console.error(`Failed to verify ${ticker.symbol}: ${error}`);
      }
    }

    console.log(`Updated ratings for ${updated} tickers`);
  }

  /**
   * Generate data quality report
   */
  async generateQualityReport(): Promise<void> {
    const report = await this.storage.getDataQualityReport();

    console.log("\n=== Ticker Data Quality Report ===");
    console.log(`Total Active Tickers: ${report.totalTickers}`);
    console.log("\nBy Source Rating:");
    for (const [rating, count] of Object.entries(report.byRating).sort()) {
      const percentage = ((count / report.totalTickers) * 100).toFixed(1);
      console.log(`  ${rating}: ${count} (${percentage}%)`);
    }

    console.log(`\nMissing First Trade Date: ${report.missingFirstTrade}`);
    console.log(`Using Default NYC Location: ${report.defaultLocation}`);
    console.log(`Needs Update (>30 days): ${report.needsUpdate}`);

    if (report.issues.length > 0) {
      console.log("\nTop Issues:");
      report.issues.slice(0, 10).forEach(({ symbol, issues }) => {
        console.log(`  ${symbol}: ${issues.join(", ")}`);
      });
    }
  }
}
