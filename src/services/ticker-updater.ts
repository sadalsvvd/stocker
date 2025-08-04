import { SECService } from './sec.js';
import { FinvizFetcher } from './finviz-fetcher.js';
import { EODHistoricalService } from './eodHistorical.js';
import { EnhancedReferenceStorage, EnhancedTickerInfo } from '../storage/enhanced-reference.js';
import type { TickerInfo } from '../types/index.js';

interface UpdateResult {
  updated: number;
  added: number;
  delisted: number;
  errors: string[];
}

export class TickerUpdaterService {
  private sec: SECService;
  private finviz: FinvizFetcher;
  private eod: EODHistoricalService;
  private storage: EnhancedReferenceStorage;

  constructor(
    storage: EnhancedReferenceStorage,
    eodApiKey?: string
  ) {
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
    console.log('Starting ticker universe update...');
    
    const result: UpdateResult = {
      updated: 0,
      added: 0,
      delisted: 0,
      errors: []
    };

    try {
      // 1. Get current tickers from storage
      const existingTickers = await this.storage.getAllTickers('active');
      const tickerMap = new Map(existingTickers.map(t => [t.symbol, t]));

      // 2. Fetch from SEC (official US tickers)
      console.log('Fetching from SEC...');
      const secTickers = await this.sec.fetchAllTickers();
      
      // 3. Fetch from Finviz (includes more metadata)
      console.log('Fetching from Finviz...');
      let finvizTickers: any[] = [];
      try {
        finvizTickers = await this.finviz.fetchAllStocks();
      } catch (error) {
        result.errors.push(`Finviz fetch failed: ${error}`);
      }

      // 4. Merge data sources
      const mergedTickers = new Map<string, EnhancedTickerInfo>();
      
      // Process SEC data first (authoritative for US stocks)
      for (const secTicker of secTickers) {
        const enhanced: EnhancedTickerInfo = {
          ...secTicker,
          securityType: 'stock',
          sourceRating: 'A', // SEC data is authoritative
          dataSources: ['SEC'],
          metadata: {
            ...secTicker.metadata,
            dataSources: ['SEC']
          }
        };
        mergedTickers.set(secTicker.symbol, enhanced);
      }

      // Enhance with Finviz data
      for (const fv of finvizTickers) {
        const existing = mergedTickers.get(fv.ticker);
        if (existing) {
          // Enhance existing ticker
          existing.sector = fv.sector || existing.sector;
          existing.industry = fv.industry || existing.industry;
          existing.country = fv.country || existing.country;
          existing.volume = fv.volume;
          existing.metadata = {
            ...existing.metadata,
            marketCap: fv.marketCap,
            dataSources: [...(existing.metadata?.dataSources || []), 'Finviz']
          };
        } else {
          // New ticker from Finviz
          const enhanced: EnhancedTickerInfo = {
            symbol: fv.ticker,
            companyName: fv.company,
            exchange: fv.exchange || 'UNKNOWN',
            sector: fv.sector,
            industry: fv.industry,
            status: 'active',
            firstSeen: new Date().toISOString().split('T')[0],
            lastUpdated: new Date().toISOString().split('T')[0],
            securityType: 'stock',
            sourceRating: 'B', // Finviz only
            country: fv.country,
            volume: fv.volume,
            metadata: {
              marketCap: fv.marketCap,
              dataSources: ['Finviz']
            }
          };
          mergedTickers.set(fv.ticker, enhanced);
        }
      }

      // 5. Find delisted tickers
      const currentSymbols = new Set(mergedTickers.keys());
      for (const [symbol, ticker] of tickerMap) {
        if (!currentSymbols.has(symbol)) {
          // Mark as delisted
          await this.storage.upsertEnhancedTicker({
            ...ticker,
            status: 'delisted',
            delistedDate: new Date().toISOString().split('T')[0],
            lastUpdated: new Date().toISOString().split('T')[0],
            metadata: {
              ...ticker.metadata,
              delistingReason: 'Not found in current sources'
            }
          });
          result.delisted++;
        }
      }

      // 6. Update or add tickers
      for (const [symbol, ticker] of mergedTickers) {
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
          
          await this.storage.upsertEnhancedTicker(ticker);
          
          if (existing) {
            result.updated++;
          } else {
            result.added++;
          }
        } catch (error) {
          result.errors.push(`Failed to update ${symbol}: ${error}`);
        }
      }

      console.log(`Update complete. Added: ${result.added}, Updated: ${result.updated}, Delisted: ${result.delisted}`);
      
    } catch (error) {
      result.errors.push(`Update failed: ${error}`);
    }

    return result;
  }

  /**
   * Find first trade date from price history
   */
  private async findFirstTradeDate(symbol: string): Promise<string | undefined> {
    if (!this.eod) return undefined;
    
    try {
      // Fetch max history
      const prices = await this.eod.fetchDaily(symbol, '1900-01-01');
      if (prices.length > 0) {
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
  private getExchangeLocation(exchange: string): { lat: number; lng: number; timezone: string } {
    const EXCHANGE_LOCATIONS: Record<string, { lat: number; lng: number; timezone: string }> = {
      NYSE: { lat: 40.7069, lng: -74.0113, timezone: 'America/New_York' },
      NASDAQ: { lat: 40.7489, lng: -73.9680, timezone: 'America/New_York' },
      AMEX: { lat: 40.7069, lng: -74.0113, timezone: 'America/New_York' },
      LSE: { lat: 51.5155, lng: -0.0922, timezone: 'Europe/London' },
      TSE: { lat: 35.6785, lng: 139.7704, timezone: 'Asia/Tokyo' },
      HKEX: { lat: 22.2855, lng: 114.1577, timezone: 'Asia/Hong_Kong' },
      SSE: { lat: 31.2304, lng: 121.4737, timezone: 'Asia/Shanghai' },
    };

    // Handle variations
    const normalized = exchange.toUpperCase().replace(/[^A-Z]/g, '');
    for (const [key, value] of Object.entries(EXCHANGE_LOCATIONS)) {
      if (normalized.includes(key)) {
        return value;
      }
    }

    // Default to NYSE
    return EXCHANGE_LOCATIONS.NYSE;
  }

  /**
   * Verify and rate ticker data quality
   */
  async verifyAndRateTickers(): Promise<void> {
    console.log('Verifying ticker data quality...');
    
    const tickers = await this.storage.getAllTickers('active');
    let updated = 0;
    
    for (const ticker of tickers) {
      try {
        // Calculate source rating based on data completeness
        let rating = 'F';
        let score = 0;
        
        // Check data completeness
        if (ticker.companyName && ticker.companyName !== ticker.symbol) score += 20;
        if (ticker.exchange && ticker.exchange !== 'UNKNOWN') score += 20;
        if (ticker.sector) score += 10;
        if (ticker.industry) score += 10;
        if (ticker.firstTradeDate || ticker.ipoDate) score += 20;
        if (ticker.metadata?.dataSources?.length > 1) score += 20;
        
        // Convert score to rating
        if (score >= 90) rating = 'A';
        else if (score >= 70) rating = 'B';
        else if (score >= 50) rating = 'C';
        else if (score >= 30) rating = 'D';
        
        // Update if rating changed
        if (ticker.sourceRating !== rating) {
          await this.storage.upsertEnhancedTicker({
            ...ticker,
            sourceRating: rating,
            lastUpdated: new Date().toISOString().split('T')[0]
          });
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
    
    console.log('\n=== Ticker Data Quality Report ===');
    console.log(`Total Active Tickers: ${report.totalTickers}`);
    console.log('\nBy Source Rating:');
    for (const [rating, count] of Object.entries(report.byRating).sort()) {
      const percentage = ((count / report.totalTickers) * 100).toFixed(1);
      console.log(`  ${rating}: ${count} (${percentage}%)`);
    }
    
    console.log(`\nMissing First Trade Date: ${report.missingFirstTrade}`);
    console.log(`Using Default NYC Location: ${report.defaultLocation}`);
    console.log(`Needs Update (>30 days): ${report.needsUpdate}`);
    
    if (report.issues.length > 0) {
      console.log('\nTop Issues:');
      report.issues.slice(0, 10).forEach(({ symbol, issues }) => {
        console.log(`  ${symbol}: ${issues.join(', ')}`);
      });
    }
  }
}