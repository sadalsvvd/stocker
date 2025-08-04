import { ReferenceStorage } from './reference.js';
import type { TickerInfo } from '../types/index.js';

// Extended ticker info for CSV data
export interface EnhancedTickerInfo extends TickerInfo {
  // Core fields from CSV
  securityType?: string;
  sourceRating?: string;
  firstTradeDate?: string;
  
  // Location data
  lat?: number;
  lng?: number;
  timezone?: string;
  
  // Additional tracking
  volume?: number;
  country?: string;
  locationRating?: string;
  sourceNote?: string;
}

export class EnhancedReferenceStorage extends ReferenceStorage {
  
  override async init(): Promise<void> {
    await super.init();
    
    // Add enhanced ticker table with all CSV fields
    await this.execute(`
      CREATE TABLE IF NOT EXISTS ticker_master (
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
        
        -- First trade tracking
        ipo_date DATE,
        first_trade_date DATE,
        source_rating VARCHAR,
        source_note TEXT,
        
        -- Location data (default NYC)
        lat DOUBLE DEFAULT 40.7069,
        lng DOUBLE DEFAULT -74.0113,
        timezone VARCHAR DEFAULT 'America/New_York',
        location_rating VARCHAR,
        
        -- Status tracking
        status VARCHAR NOT NULL DEFAULT 'active',
        delisted_date DATE,
        delisting_reason VARCHAR,
        previous_symbols JSON,
        successor_symbol VARCHAR,
        
        -- Trading data
        last_price_date DATE,
        avg_volume DOUBLE,
        
        -- Metadata
        country VARCHAR,
        data_sources JSON,
        first_seen DATE NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        notes TEXT,
        metadata JSON
      )
    `);
    
    // Create indexes for better performance
    await this.execute(`
      CREATE INDEX IF NOT EXISTS idx_ticker_exchange ON ticker_master(exchange);
      CREATE INDEX IF NOT EXISTS idx_ticker_status ON ticker_master(status);
      CREATE INDEX IF NOT EXISTS idx_ticker_rating ON ticker_master(source_rating);
      CREATE INDEX IF NOT EXISTS idx_ticker_first_trade ON ticker_master(first_trade_date);
    `);
  }
  
  async upsertEnhancedTicker(ticker: EnhancedTickerInfo): Promise<void> {
    const metadata = ticker.metadata || {};
    
    await this.execute(`
      INSERT OR REPLACE INTO ticker_master (
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
      ticker.symbol,
      ticker.figi || null,
      ticker.cusip || null,
      ticker.isin || null,
      ticker.companyName,
      ticker.exchange,
      metadata.exchangeCountry || null,
      ticker.sector || null,
      ticker.industry || null,
      ticker.securityType || metadata.securityType || 'stocks',
      ticker.ipoDate || null,
      ticker.firstTradeDate || metadata.firstTradeDate || null,
      ticker.sourceRating || metadata.sourceRating || null,
      ticker.sourceNote || metadata.sourceNote || null,
      ticker.lat || metadata.lat || 40.7069,
      ticker.lng || metadata.lng || -74.0113,
      ticker.timezone || metadata.timezone || 'America/New_York',
      ticker.locationRating || metadata.locationRating || null,
      ticker.status,
      ticker.delistedDate || null,
      metadata.delistingReason || null,
      JSON.stringify(metadata.previousSymbols || []),
      metadata.successorSymbol || null,
      metadata.lastPriceDate || null,
      ticker.volume || metadata.volume || null,
      ticker.country || metadata.country || null,
      JSON.stringify(metadata.dataSources || []),
      ticker.firstSeen,
      ticker.lastUpdated,
      metadata.notes || null,
      JSON.stringify(metadata)
    );
  }
  
  async getEnhancedTicker(symbol: string): Promise<EnhancedTickerInfo | null> {
    const result = await this.queryOne<any>(`
      SELECT * FROM ticker_master WHERE symbol = ?
    `, symbol);
    
    if (!result) return null;
    
    return this.mapRowToEnhancedTicker(result);
  }
  
  async getTickersBySourceRating(rating: string): Promise<EnhancedTickerInfo[]> {
    const results = await this.query<any>(`
      SELECT * FROM ticker_master 
      WHERE source_rating = ? 
      ORDER BY symbol
    `, rating);
    
    return results.map(row => this.mapRowToEnhancedTicker(row));
  }
  
  async getTickersNeedingUpdate(days: number = 30): Promise<EnhancedTickerInfo[]> {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days);
    
    const results = await this.query<any>(`
      SELECT * FROM ticker_master
      WHERE last_updated < ?
      ORDER BY last_updated ASC, symbol
    `, cutoffDate.toISOString().split('T')[0]);
    
    return results.map(row => this.mapRowToEnhancedTicker(row));
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
      FROM ticker_master
      WHERE status = 'active'
      ORDER BY symbol
    `);
    
    // Build CSV
    const headers = [
      'symbol', 'security_type', 'source_rating', 'datetime',
      'company', 'lat', 'lng', 'sector', 'industry', 'country',
      'volume', 'exchange', 'location_rating', 'source_note'
    ];
    
    const rows = results.map(row => {
      return headers.map(header => {
        const value = row[header] ?? '';
        // Escape quotes and wrap in quotes if contains comma
        const escaped = String(value).replace(/"/g, '""');
        return escaped.includes(',') ? `"${escaped}"` : escaped;
      }).join(',');
    });
    
    return [headers.join(','), ...rows].join('\n');
  }
  
  async getDataQualityReport(): Promise<{
    totalTickers: number;
    byRating: Record<string, number>;
    missingFirstTrade: number;
    defaultLocation: number;
    needsUpdate: number;
    issues: Array<{ symbol: string; issues: string[] }>;
  }> {
    const allTickers = await this.query<any>(`
      SELECT * FROM ticker_master WHERE status = 'active'
    `);
    
    const report = {
      totalTickers: allTickers.length,
      byRating: {} as Record<string, number>,
      missingFirstTrade: 0,
      defaultLocation: 0,
      needsUpdate: 0,
      issues: [] as Array<{ symbol: string; issues: string[] }>
    };
    
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    
    for (const ticker of allTickers) {
      const issues: string[] = [];
      
      // Count by rating
      if (ticker.source_rating) {
        report.byRating[ticker.source_rating] = (report.byRating[ticker.source_rating] || 0) + 1;
      }
      
      // Check for missing first trade date
      if (!ticker.first_trade_date && !ticker.ipo_date) {
        report.missingFirstTrade++;
        issues.push('No first trade or IPO date');
      }
      
      // Check for default NYC location on non-US exchanges
      if (ticker.lat === 40.7069 && ticker.lng === -74.0113) {
        if (ticker.exchange && !['NYSE', 'NASDAQ', 'AMEX'].includes(ticker.exchange)) {
          report.defaultLocation++;
          issues.push(`Using NYC location for ${ticker.exchange}`);
        }
      }
      
      // Check if needs update
      if (new Date(ticker.last_updated) < thirtyDaysAgo) {
        report.needsUpdate++;
        issues.push('Data older than 30 days');
      }
      
      // Check poor ratings
      if (ticker.source_rating && ['C', 'D', 'F'].includes(ticker.source_rating)) {
        issues.push(`Poor source rating: ${ticker.source_rating}`);
      }
      
      if (issues.length > 0) {
        report.issues.push({ symbol: ticker.symbol, issues });
      }
    }
    
    // Sort issues by number of problems
    report.issues.sort((a, b) => b.issues.length - a.issues.length);
    
    return report;
  }
  
  private mapRowToEnhancedTicker(row: any): EnhancedTickerInfo {
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
      
      // Enhanced fields
      securityType: row.security_type,
      sourceRating: row.source_rating,
      firstTradeDate: row.first_trade_date,
      lat: row.lat,
      lng: row.lng,
      timezone: row.timezone,
      volume: row.avg_volume,
      country: row.country,
      locationRating: row.location_rating,
      sourceNote: row.source_note,
      
      metadata: {
        ...JSON.parse(row.metadata || '{}'),
        exchangeCountry: row.exchange_country,
        delistingReason: row.delisting_reason,
        previousSymbols: JSON.parse(row.previous_symbols || '[]'),
        successorSymbol: row.successor_symbol,
        lastPriceDate: row.last_price_date,
        dataSources: JSON.parse(row.data_sources || '[]'),
        notes: row.notes
      }
    };
  }
}