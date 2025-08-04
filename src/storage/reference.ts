import { DuckDBBase } from "./base";
import type { TickerInfo, IPOEvent } from "../types/index";
import { join } from "path";
import { existsSync } from "fs";

export class ReferenceStorage extends DuckDBBase {
  private referenceDir: string;

  constructor(dataDir: string) {
    super(dataDir);
    this.referenceDir = join(dataDir, "reference");
  }

  override async init(): Promise<void> {
    await super.init();

    // Create reference tables
    await this.execute(`
      CREATE TABLE IF NOT EXISTS ticker_registry (
        symbol VARCHAR PRIMARY KEY,
        figi VARCHAR,
        cusip VARCHAR,
        isin VARCHAR,
        company_name VARCHAR NOT NULL,
        exchange VARCHAR NOT NULL,
        sector VARCHAR,
        industry VARCHAR,
        ipo_date DATE,
        delisted_date DATE,
        status VARCHAR NOT NULL,
        first_seen DATE NOT NULL,
        last_updated DATE NOT NULL,
        metadata JSON
      )
    `);

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

  async upsertTickers(tickers: TickerInfo[]): Promise<void> {
    if (tickers.length === 0) return;

    console.log(`Upserting ${tickers.length} tickers...`);

    // Prepare data for bulk insert
    const values = tickers
      .map(
        (t) =>
          `('${t.symbol}', ${t.figi ? `'${t.figi}'` : "NULL"}, ` +
          `${t.cusip ? `'${t.cusip}'` : "NULL"}, ${t.isin ? `'${t.isin}'` : "NULL"}, ` +
          `'${t.companyName.replace(/'/g, "''")}', '${t.exchange}', ` +
          `${t.sector ? `'${t.sector.replace(/'/g, "''")}'` : "NULL"}, ` +
          `${t.industry ? `'${t.industry.replace(/'/g, "''")}'` : "NULL"}, ` +
          `${t.ipoDate ? `'${t.ipoDate}'` : "NULL"}, ` +
          `${t.delistedDate ? `'${t.delistedDate}'` : "NULL"}, ` +
          `'${t.status}', '${t.firstSeen}', '${t.lastUpdated}', ` +
          `'${JSON.stringify(t.metadata || {}).replace(/'/g, "''")}')`
      )
      .join(", ");

    // Use INSERT OR REPLACE for upsert behavior
    await this.execute(`
      INSERT OR REPLACE INTO ticker_registry 
      VALUES ${values}
    `);

    // Export to parquet for persistence
    await this.exportTickersToParquet();
  }

  async getTicker(symbol: string): Promise<TickerInfo | null> {
    const result = await this.queryOne<any>(
      `SELECT * FROM ticker_registry WHERE symbol = ?`,
      symbol
    );

    if (!result) return null;

    return this.mapRowToTickerInfo(result);
  }

  async getAllTickers(status?: "active" | "delisted"): Promise<TickerInfo[]> {
    let query = "SELECT * FROM ticker_registry";
    if (status) {
      query += ` WHERE status = '${status}'`;
    }
    query += " ORDER BY symbol";

    const results = await this.query<any>(query);
    return results.map((row) => this.mapRowToTickerInfo(row));
  }

  async searchTickers(query: string): Promise<TickerInfo[]> {
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

  private mapRowToTickerInfo(row: any): TickerInfo {
    return {
      symbol: row.symbol,
      figi: row.figi || undefined,
      cusip: row.cusip || undefined,
      isin: row.isin || undefined,
      companyName: row.company_name,
      exchange: row.exchange,
      sector: row.sector || undefined,
      industry: row.industry || undefined,
      ipoDate: row.ipo_date || undefined,
      delistedDate: row.delisted_date || undefined,
      status: row.status,
      firstSeen: row.first_seen,
      lastUpdated: row.last_updated,
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

  async close(): Promise<void> {
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }
}