import { DuckDBBase } from "./base";
import type { TickerInfo, IPOEvent, CorporateAction } from "../types/index";
import { join } from "path";
import { existsSync } from "fs";

// Extended ticker info for comprehensive tracking
export interface TickerMaster extends TickerInfo {
  // Additional fields for enhanced tracking
  securityType?: string;
  exchangeCountry?: string;

  // First trade tracking
  firstTradeDate?: string;
  firstDataSource?: string;
  pricingDate?: string;
  offerPrice?: number;

  // Location data - flat structure for CSV compatibility
  // Default to NYSE if not specified: 40.7069, -74.0113
  lat?: number;
  lng?: number;
  timezone?: string;

  // Data quality
  sourceRating?: string; // 'A', 'B', 'C', 'D', 'F'
  dataSources?: string[];

  // Status tracking
  delistingReason?: string;
  previousSymbols?: string[];
  successorSymbol?: string;
  lastPriceDate?: string;
  notes?: string;

  // Trading data
  volume?: number;
  country?: string;
  locationRating?: string;
  sourceNote?: string;
}

// NYC default coordinates
const NYC_LAT = 40.7069;
const NYC_LNG = -74.0113;
const NYC_TIMEZONE = "America/New_York";

// Exchange locations for reference
export const EXCHANGE_LOCATIONS = {
  NYSE: { lat: 40.7069, lng: -74.0113, timezone: "America/New_York" },
  NASDAQ: { lat: 40.7489, lng: -73.968, timezone: "America/New_York" },
  AMEX: { lat: 40.7069, lng: -74.0113, timezone: "America/New_York" },
  LSE: { lat: 51.5155, lng: -0.0922, timezone: "Europe/London" },
  TSE: { lat: 35.6785, lng: 139.7704, timezone: "Asia/Tokyo" },
  HKEX: { lat: 22.2855, lng: 114.1577, timezone: "Asia/Hong_Kong" },
  SSE: { lat: 31.2304, lng: 121.4737, timezone: "Asia/Shanghai" },
} as const;

export class ReferenceStorage extends DuckDBBase {
  private referenceDir: string;

  constructor(dataDir: string) {
    super(dataDir);
    this.referenceDir = join(dataDir, "reference");
  }

  override async init(): Promise<void> {
    await super.init();

    try {
      // Drop the old table if it exists with different schema
      // This is safe since we're storing data in Parquet files
      await this.execute(`DROP TABLE IF EXISTS ticker_registry`);
    } catch (e) {
      // Ignore errors from dropping
    }

    // Create enhanced ticker table with all fields
    await this.execute(`
      CREATE TABLE IF NOT EXISTS ticker_registry (
        symbol VARCHAR PRIMARY KEY,
        figi VARCHAR,
        cusip VARCHAR,
        isin VARCHAR,
        company_name VARCHAR NOT NULL,
        exchange VARCHAR NOT NULL,
        exchange_country VARCHAR,
        sector VARCHAR,
        industry VARCHAR,
        security_type VARCHAR DEFAULT 'stocks',
        ipo_date DATE,
        first_trade_date DATE,
        source_rating VARCHAR,
        source_note TEXT,
        lat DOUBLE DEFAULT 40.7069,
        lng DOUBLE DEFAULT -74.0113,
        timezone VARCHAR DEFAULT 'America/New_York',
        location_rating VARCHAR,
        status VARCHAR NOT NULL DEFAULT 'active',
        delisted_date DATE,
        delisting_reason VARCHAR,
        previous_symbols JSON,
        successor_symbol VARCHAR,
        last_price_date DATE,
        avg_volume DOUBLE,
        country VARCHAR,
        data_sources JSON,
        first_seen DATE NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        notes TEXT,
        metadata JSON
      )
    `);

    // Create indexes for better performance (one at a time)
    await this.execute(
      `CREATE INDEX IF NOT EXISTS idx_ticker_exchange ON ticker_registry(exchange)`
    );
    await this.execute(
      `CREATE INDEX IF NOT EXISTS idx_ticker_status ON ticker_registry(status)`
    );
    await this.execute(
      `CREATE INDEX IF NOT EXISTS idx_ticker_rating ON ticker_registry(source_rating)`
    );
    await this.execute(
      `CREATE INDEX IF NOT EXISTS idx_ticker_first_trade ON ticker_registry(first_trade_date)`
    );

    await this.execute(`
      CREATE TABLE IF NOT EXISTS ipo_events (
        symbol VARCHAR,
        filing_date DATE,
        price_date DATE,
        ipo_date DATE NOT NULL,
        offer_price DOUBLE,
        open_price DOUBLE,
        close_price DOUBLE,
        shares_offered BIGINT,
        lead_underwriter VARCHAR,
        status VARCHAR NOT NULL,
        prospectus_url VARCHAR,
        PRIMARY KEY (symbol, ipo_date)
      )
    `);

    // Create views if parquet files exist
    await this.createReferenceViews();
  }

  private async createReferenceViews(): Promise<void> {
    const tickersPath = join(this.referenceDir, "tickers.parquet");
    const ipoPath = join(this.referenceDir, "ipo_events.parquet");

    if (existsSync(tickersPath)) {
      await this.execute(`
        CREATE OR REPLACE VIEW v_tickers AS
        SELECT * FROM read_parquet('${tickersPath}')
      `);
    }

    if (existsSync(ipoPath)) {
      await this.execute(`
        CREATE OR REPLACE VIEW v_ipo_events AS
        SELECT * FROM read_parquet('${ipoPath}')
      `);
    }
  }

  async upsertTickers(tickers: TickerMaster[]): Promise<void> {
    if (tickers.length === 0) return;

    if (tickers.length > 10) {
      console.log(
        `Upserting ${tickers.length} tickers: ${tickers
          .slice(0, 5)
          .map((t) => t.symbol)
          .join(", ")}... and ${tickers.length - 5} more`
      );
    } else {
      console.log(
        `Upserting ${tickers.length} ticker${tickers.length === 1 ? '' : 's'}: ${tickers
          .map((t) => t.symbol)
          .join(", ")}`
      );
    }

    // Use parameterized insert for better safety
    for (const ticker of tickers) {
      // Ensure defaults
      const t: TickerMaster = {
        ...ticker,
        lat: ticker.lat ?? NYC_LAT,
        lng: ticker.lng ?? NYC_LNG,
        timezone: ticker.timezone ?? NYC_TIMEZONE,
        lastUpdated:
          ticker.lastUpdated || new Date().toISOString().split("T")[0],
      };

      await this.execute(
        `
        INSERT OR REPLACE INTO ticker_registry (
          symbol, figi, cusip, isin, company_name, exchange, exchange_country,
          sector, industry, security_type, ipo_date, first_trade_date,
          source_rating, source_note, lat, lng, timezone, location_rating,
          status, delisted_date, delisting_reason, previous_symbols, successor_symbol,
          last_price_date, avg_volume, country, data_sources,
          first_seen, last_updated, notes, metadata
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
      `,
        t.symbol,
        t.figi || null,
        t.cusip || null,
        t.isin || null,
        t.companyName,
        t.exchange,
        t.exchangeCountry || null,
        t.sector || null,
        t.industry || null,
        t.securityType || t.metadata?.securityType || "stocks",
        t.ipoDate || null,
        t.firstTradeDate || t.metadata?.firstTradeDate || null,
        t.sourceRating || t.metadata?.sourceRating || null,
        t.sourceNote || t.metadata?.sourceNote || null,
        t.lat,
        t.lng,
        t.timezone,
        t.locationRating || t.metadata?.locationRating || null,
        t.status,
        t.delistedDate || null,
        t.delistingReason || t.metadata?.delistingReason || null,
        JSON.stringify(t.previousSymbols || t.metadata?.previousSymbols || []),
        t.successorSymbol || t.metadata?.successorSymbol || null,
        t.lastPriceDate || t.metadata?.lastPriceDate || null,
        t.volume || t.metadata?.volume || null,
        t.country || t.metadata?.country || null,
        JSON.stringify(t.dataSources || t.metadata?.dataSources || []),
        t.firstSeen,
        t.lastUpdated,
        t.notes || t.metadata?.notes || null,
        JSON.stringify(t.metadata || {})
      );
    }

    // Export to parquet for persistence
    await this.exportTickersToParquet();
  }

  async getTicker(symbol: string): Promise<TickerMaster | null> {
    const result = await this.queryOne<any>(
      `SELECT * FROM ticker_registry WHERE symbol = ?`,
      symbol
    );

    if (!result) return null;

    return this.mapRowToTickerInfo(result);
  }

  async getAllTickers(status?: "active" | "delisted"): Promise<TickerMaster[]> {
    let query = "SELECT * FROM ticker_registry";
    if (status) {
      query += ` WHERE status = '${status}'`;
    }
    query += " ORDER BY symbol";

    const results = await this.query<any>(query);
    return results.map((row) => this.mapRowToTickerInfo(row));
  }

  async searchTickers(query: string): Promise<TickerMaster[]> {
    const searchTerm = `%${query.toUpperCase()}%`;
    const results = await this.query<any>(
      `
      SELECT * FROM ticker_registry 
      WHERE UPPER(symbol) LIKE ? 
         OR UPPER(company_name) LIKE ?
      ORDER BY 
        CASE 
          WHEN UPPER(symbol) = ? THEN 0
          WHEN UPPER(symbol) LIKE ? THEN 1
          ELSE 2
        END,
        symbol
      LIMIT 50
    `,
      searchTerm,
      searchTerm,
      query.toUpperCase(),
      `${query.toUpperCase()}%`
    );

    return results.map((row) => this.mapRowToTickerInfo(row));
  }

  async getTickerStats(): Promise<{
    total: number;
    active: number;
    delisted: number;
    byExchange: Record<string, number>;
  }> {
    const stats = await this.queryOne<any>(`
      SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN status = 'active' THEN 1 END) as active,
        COUNT(CASE WHEN status = 'delisted' THEN 1 END) as delisted
      FROM ticker_registry
    `);

    const byExchange = await this.query<{ exchange: string; count: number }>(`
      SELECT exchange, COUNT(*) as count
      FROM ticker_registry
      GROUP BY exchange
      ORDER BY count DESC
    `);

    return {
      total: stats?.total || 0,
      active: stats?.active || 0,
      delisted: stats?.delisted || 0,
      byExchange: Object.fromEntries(
        byExchange.map((row) => [row.exchange, row.count])
      ),
    };
  }

  private async exportTickersToParquet(): Promise<void> {
    const tickersPath = join(this.referenceDir, "tickers.parquet");

    await this.execute(`
      COPY (
        SELECT * FROM ticker_registry 
        ORDER BY symbol
      ) TO '${tickersPath}' 
      (FORMAT PARQUET, COMPRESSION 'ZSTD')
    `);

    // Recreate view
    await this.createReferenceViews();
  }

  private mapRowToTickerInfo(row: any): TickerMaster {
    return {
      symbol: row.symbol,
      figi: row.figi || undefined,
      cusip: row.cusip || undefined,
      isin: row.isin || undefined,
      companyName: row.company_name,
      exchange: row.exchange,
      exchangeCountry: row.exchange_country || undefined,
      sector: row.sector || undefined,
      industry: row.industry || undefined,
      securityType: row.security_type || undefined,
      ipoDate: row.ipo_date || undefined,
      firstTradeDate: row.first_trade_date || undefined,
      sourceRating: row.source_rating || undefined,
      sourceNote: row.source_note || undefined,
      lat: row.lat,
      lng: row.lng,
      timezone: row.timezone,
      locationRating: row.location_rating || undefined,
      delistedDate: row.delisted_date || undefined,
      delistingReason: row.delisting_reason || undefined,
      previousSymbols: row.previous_symbols
        ? JSON.parse(row.previous_symbols)
        : undefined,
      successorSymbol: row.successor_symbol || undefined,
      lastPriceDate: row.last_price_date || undefined,
      volume: row.avg_volume || undefined,
      country: row.country || undefined,
      dataSources: row.data_sources ? JSON.parse(row.data_sources) : undefined,
      status: row.status,
      firstSeen: row.first_seen,
      lastUpdated: row.last_updated,
      notes: row.notes || undefined,
      metadata: row.metadata ? JSON.parse(row.metadata) : undefined,
    };
  }

  // IPO Event methods
  async addIPOEvent(event: IPOEvent): Promise<void> {
    await this.execute(
      `
      INSERT OR REPLACE INTO ipo_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
      event.symbol,
      event.filingDate || null,
      event.priceDate || null,
      event.ipoDate,
      event.offerPrice || null,
      event.openPrice || null,
      event.closePrice || null,
      event.sharesOffered || null,
      event.leadUnderwriter || null,
      event.status,
      event.prospectusUrl || null
    );
  }

  async getIPOEvents(symbol?: string): Promise<IPOEvent[]> {
    let query = "SELECT * FROM ipo_events";
    if (symbol) {
      query += ` WHERE symbol = '${symbol}'`;
    }
    query += " ORDER BY ipo_date DESC";

    const results = await this.query<any>(query);
    return results.map((row) => ({
      symbol: row.symbol,
      filingDate: row.filing_date || undefined,
      priceDate: row.price_date || undefined,
      ipoDate: row.ipo_date,
      offerPrice: row.offer_price || undefined,
      openPrice: row.open_price || undefined,
      closePrice: row.close_price || undefined,
      sharesOffered: row.shares_offered || undefined,
      leadUnderwriter: row.lead_underwriter || undefined,
      status: row.status,
      prospectusUrl: row.prospectus_url || undefined,
    }));
  }

  // Additional query methods
  async getTickersBySourceRating(rating: string): Promise<TickerMaster[]> {
    const results = await this.query<any>(
      `
      SELECT * FROM ticker_registry 
      WHERE source_rating = ? 
      ORDER BY symbol
    `,
      rating
    );

    return results.map((row) => this.mapRowToTickerInfo(row));
  }

  async getTickersNeedingUpdate(days: number = 30): Promise<TickerMaster[]> {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days);

    const results = await this.query<any>(
      `
      SELECT * FROM ticker_registry
      WHERE last_updated < ?
      ORDER BY last_updated ASC, symbol
    `,
      cutoffDate.toISOString().split("T")[0]
    );

    return results.map((row) => this.mapRowToTickerInfo(row));
  }

  // CSV import/export methods
  async importFromCSV(csvData: any[]): Promise<{
    imported: number;
    skipped: number;
    errors: string[];
  }> {
    let imported = 0;
    let skipped = 0;
    const errors: string[] = [];
    const tickers: TickerMaster[] = [];

    for (const row of csvData) {
      try {
        // Skip delisted entries (prefixed with #)
        if (row.symbol?.startsWith("#")) {
          skipped++;
          continue;
        }

        // Parse coordinates
        let lat = NYC_LAT;
        let lng = NYC_LNG;

        if (row.lat && row.lat !== "NY_LAT") {
          lat = parseFloat(row.lat) || NYC_LAT;
        }
        if (row.lng && row.lng !== "NY_LNG") {
          lng = parseFloat(row.lng) || NYC_LNG;
        }

        // Convert CSV format to TickerMaster
        const ticker: TickerMaster = {
          symbol: row.symbol,
          companyName: row.company || row.symbol,
          exchange: row.exchange || "UNKNOWN",
          status: "active",
          firstSeen:
            row.datetime ||
            row.first_seen ||
            new Date().toISOString().split("T")[0],
          lastUpdated: new Date().toISOString().split("T")[0],

          // Map first trade date
          firstTradeDate: row.datetime,
          firstDataSource: row.source_note || "legacy_csv",

          // Location with parsed coordinates
          lat,
          lng,
          timezone: NYC_TIMEZONE,

          // Map other fields
          sector: row.sector,
          industry: row.industry,
          sourceRating: row.source_rating,

          metadata: {
            volume: row.volume,
            locationRating: row.location_rating,
            country: row.country,
            securityType: row.security_type,
            originalCsv: true,
          },
        };

        tickers.push(ticker);
        imported++;
      } catch (error) {
        errors.push(`Failed to import ${row.symbol}: ${error}`);
      }
    }

    // Bulk insert
    if (tickers.length > 0) {
      await this.upsertTickers(tickers);
    }

    return { imported, skipped, errors };
  }

  async exportToCSV(): Promise<string> {
    const results = await this.query<any>(`
      SELECT 
        symbol,
        security_type,
        source_rating,
        COALESCE(first_trade_date, ipo_date) as datetime,
        company_name as company,
        lat,
        lng,
        sector,
        industry,
        country,
        avg_volume as volume,
        exchange,
        location_rating,
        source_note
      FROM ticker_registry
      WHERE status = 'active'
      ORDER BY symbol
    `);

    // Build CSV
    const headers = [
      "symbol",
      "security_type",
      "source_rating",
      "datetime",
      "company",
      "lat",
      "lng",
      "sector",
      "industry",
      "country",
      "volume",
      "exchange",
      "location_rating",
      "source_note",
    ];

    const rows = results.map((row) => {
      return headers
        .map((header) => {
          const value = row[header] ?? "";
          // Escape quotes and wrap in quotes if contains comma
          const escaped = String(value).replace(/"/g, '""');
          return escaped.includes(",") ? `"${escaped}"` : escaped;
        })
        .join(",");
    });

    return [headers.join(","), ...rows].join("\n");
  }

  // Data quality report
  async getDataQualityReport(): Promise<{
    totalTickers: number;
    byRating: Record<string, number>;
    missingFirstTrade: number;
    defaultLocation: number;
    needsUpdate: number;
    issues: Array<{ symbol: string; issues: string[] }>;
  }> {
    const allTickers = await this.getAllTickers("active");

    const report = {
      totalTickers: allTickers.length,
      byRating: {} as Record<string, number>,
      missingFirstTrade: 0,
      defaultLocation: 0,
      needsUpdate: 0,
      issues: [] as Array<{ symbol: string; issues: string[] }>,
    };

    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    for (const ticker of allTickers) {
      const issues: string[] = [];

      // Count by rating
      if (ticker.sourceRating) {
        report.byRating[ticker.sourceRating] =
          (report.byRating[ticker.sourceRating] || 0) + 1;
      }

      // Check for missing first trade date
      if (!ticker.firstTradeDate && !ticker.ipoDate) {
        report.missingFirstTrade++;
        issues.push("No first trade or IPO date");
      }

      // Check for default NYC location on non-US exchanges
      if (ticker.lat === NYC_LAT && ticker.lng === NYC_LNG) {
        if (
          ticker.exchange &&
          !["NYSE", "NASDAQ", "AMEX"].includes(ticker.exchange)
        ) {
          report.defaultLocation++;
          issues.push(`Using NYC location for ${ticker.exchange}`);
        }
      }

      // Check if needs update
      if (new Date(ticker.lastUpdated) < thirtyDaysAgo) {
        report.needsUpdate++;
        issues.push("Data older than 30 days");
      }

      // Check poor ratings
      if (
        ticker.sourceRating &&
        ["C", "D", "F"].includes(ticker.sourceRating)
      ) {
        issues.push(`Poor source rating: ${ticker.sourceRating}`);
      }

      if (issues.length > 0) {
        report.issues.push({ symbol: ticker.symbol, issues });
      }
    }

    // Sort issues by number of problems
    report.issues.sort((a, b) => b.issues.length - a.issues.length);

    return report;
  }

  // Exchange location helper
  getExchangeCoordinates(exchange: string): {
    lat: number;
    lng: number;
    timezone: string;
  } {
    // Handle common variations
    const normalizedExchange = exchange.toUpperCase().replace(/[^A-Z]/g, "");

    // Map common aliases
    const exchangeMap: Record<string, keyof typeof EXCHANGE_LOCATIONS> = {
      NYSE: "NYSE",
      NASDAQ: "NASDAQ",
      NASDAQGS: "NASDAQ",
      NASDAQGM: "NASDAQ",
      NASDAQCM: "NASDAQ",
      AMEX: "AMEX",
      ARCA: "NYSE", // NYSE Arca
      BATS: "NYSE", // Now part of NYSE
      LSE: "LSE",
      TSE: "TSE",
      HKEX: "HKEX",
      SSE: "SSE",
    };

    const mappedExchange = exchangeMap[normalizedExchange];
    const location = mappedExchange ? EXCHANGE_LOCATIONS[mappedExchange] : null;

    // Default to NYC if unknown
    return location || { lat: NYC_LAT, lng: NYC_LNG, timezone: NYC_TIMEZONE };
  }

  async close(): Promise<void> {
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }
}
