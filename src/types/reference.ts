export interface TickerMaster {
  // Core identifiers
  symbol: string;
  figi?: string;
  cusip?: string;
  isin?: string;
  
  // Company information
  companyName: string;
  exchange: string;
  exchangeCountry: string;
  
  // Classification
  sector?: string;
  industry?: string;
  securityType: 'stock' | 'etf' | 'adr' | 'reit' | 'closed-end-fund';
  
  // IPO and listing data
  ipoDate?: string;           // Actual IPO date from SEC filings
  firstTradeDate?: string;    // First available price in data sources
  firstDataSource?: string;   // Which source provided first trade
  pricingDate?: string;       // IPO pricing date
  offerPrice?: number;        // IPO offer price
  
  // Location data (for true financial astrology)
  ipoExchange?: string;       // Actual exchange where IPO occurred
  ipoLocation?: {
    city: string;
    country: string;
    latitude: number;
    longitude: number;
    timezone: string;
  };
  
  // Data quality
  sourceRating: 'A' | 'B' | 'C' | 'D' | 'F';  // Enhanced rating system
  sourceAgreement?: {
    eod?: string;
    yahoo?: string;
    alphavantage?: string;
    sec?: string;
  };
  dataSources: string[];      // All sources that have this ticker
  
  // Status tracking
  status: 'active' | 'delisted' | 'suspended' | 'merged' | 'renamed';
  delistedDate?: string;
  delistingReason?: string;
  
  // Corporate actions
  previousSymbols?: string[];  // For tracking renames
  successorSymbol?: string;    // What it became after merger/rename
  
  // Metadata
  firstSeen: string;          // When we first saw this ticker
  lastUpdated: string;        // Last verification date
  lastPriceDate?: string;     // Most recent price data
  notes?: string;             // Manual notes/warnings
}

export interface CorporateAction {
  symbol: string;
  date: string;
  actionType: 'split' | 'dividend' | 'merger' | 'spinoff' | 'rename' | 'delisting';
  details: {
    oldSymbol?: string;
    newSymbol?: string;
    ratio?: number;         // For splits
    amount?: number;        // For dividends
    currency?: string;
    description?: string;
  };
  source: string;
  verified: boolean;
}

export interface IPOEvent {
  symbol: string;
  
  // Timeline
  s1FilingDate?: string;      // Initial S-1 filing
  s1aFilingDates?: string[];  // Amendments
  roadshowDate?: string;      // Start of roadshow
  pricingDate?: string;       // When priced
  tradingDate: string;        // First trading day
  
  // Offering details
  leadUnderwriters?: string[];
  offerPrice?: number;
  priceRange?: { low: number; high: number };
  sharesOffered?: number;
  grossProceeds?: number;
  
  // Performance
  openPrice?: number;
  firstDayClose?: number;
  firstDayReturn?: number;
  
  // Metadata
  cik?: string;              // SEC CIK number
  prospectusUrl?: string;
  spac?: boolean;
  directListing?: boolean;
}

// Source quality ratings
export const SOURCE_RATINGS = {
  A: 'All sources agree within 1 day',
  B: 'Sources agree within 2-5 days',
  C: 'Sources differ by 6-30 days',
  D: 'Major discrepancies or known issues',
  F: 'Failed verification or suspicious data'
} as const;

// Exchange locations for proper astrology calculations
export const EXCHANGE_LOCATIONS = {
  NYSE: { city: 'New York', country: 'US', latitude: 40.7069, longitude: -74.0113, timezone: 'America/New_York' },
  NASDAQ: { city: 'New York', country: 'US', latitude: 40.7489, longitude: -73.9680, timezone: 'America/New_York' },
  LSE: { city: 'London', country: 'UK', latitude: 51.5155, longitude: -0.0922, timezone: 'Europe/London' },
  TSE: { city: 'Tokyo', country: 'JP', latitude: 35.6785, longitude: 139.7704, timezone: 'Asia/Tokyo' },
  HKEX: { city: 'Hong Kong', country: 'HK', latitude: 22.2855, longitude: 114.1577, timezone: 'Asia/Hong_Kong' },
  SSE: { city: 'Shanghai', country: 'CN', latitude: 31.2304, longitude: 121.4737, timezone: 'Asia/Shanghai' },
  // Add more exchanges as needed
} as const;