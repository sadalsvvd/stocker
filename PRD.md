# Product Requirements Document: Stocker

## Overview
Stocker is a TypeScript-based CLI tool for fetching, storing, and managing historical stock market data using a modern data architecture built on DuckDB and Parquet files. It serves as both a data fetching tool and a versioned data store for long-term financial datasets.

## Core Objectives
1. Port existing Python stock data fetching functionality to TypeScript
2. Implement a DuckDB-based storage layer using Parquet files
3. Create a git-versioned data store for historical stock data
4. Provide a clean CLI interface using Clipanion
5. Establish a foundation for future technical analysis features

## Architecture Overview

### Technology Stack
- **Runtime**: Bun (for performance and built-in TypeScript support)
- **CLI Framework**: Clipanion (type-safe command parsing)
- **Database**: DuckDB (embedded analytical database)
- **Storage Format**: Parquet (columnar storage for time-series data)
- **Version Control**: Git (for data versioning)

### Data Storage Structure
```
data/                       # Project-relative data directory (git-tracked)
├── catalog.duckdb          # DuckDB catalog (views, metadata)
├── catalog.duckdb.wal      # DuckDB write-ahead log (gitignored)
└── stocks/
    ├── AAPL/
    │   └── daily.parquet   # ~23KB for 1 year of complete OHLCV data
    ├── MSFT/
    │   └── daily.parquet
    └── [ticker]/
        └── daily.parquet

~/.stocker/config.yml       # User configuration file
```

## Core Components

### 1. Configuration Management ✅
- **File**: `src/config.ts`
- **Features**:
  - Load configuration from `~/.stocker/config.yml`
  - Environment variable overrides
  - API key management (EOD Historical Data)
  - Default paths and settings
  - Runtime configuration overrides via constructor

### 2. Data Services
Port the following Python services to TypeScript:

#### EOD Historical Data Service ✅
- **File**: `src/services/eodHistorical.ts`
- **Endpoints**:
  - End-of-day price data
  - Stock splits data
- **Features**:
  - Error handling with specific messages
  - Response transformation to common format
  - Calculates adjusted OHLC prices from adjustment factor

#### Yahoo Finance Service ⏸️
- **File**: `src/services/yahoo.ts`
- **Status**: Deferred (EOD Historical is sufficient for V1)

### 3. Storage Provider ✅
- **Architecture**: Modular design with base DuckDB class and stock-specific implementation
- **Files**: 
  - `src/storage/base.ts` - Generic DuckDB/Parquet operations
  - `src/storage/stocks.ts` - Stock-specific storage logic
- **Interface**:
```typescript
interface StorageProvider {
  // Initialize database and create views
  init(): Promise<void>
  
  // Fetch operations
  getDaily(ticker: string, startDate?: Date, endDate?: Date): Promise<OHLCV[]>
  exists(ticker: string): Promise<boolean>
  listTickers(): Promise<string[]>
  
  // Write operations
  writeDaily(ticker: string, data: OHLCV[]): Promise<void>
  mergeDaily(ticker: string, newData: OHLCV[]): Promise<void>
  
  // Metadata
  getLastUpdate(ticker: string): Promise<Date | null>
}
```

### 4. Data Types
```typescript
interface OHLCV {
  date: string;          // YYYY-MM-DD format
  open: number;
  high: number;
  low: number;
  close: number;
  adjustedClose: number;
  volume: number;
  split?: boolean;
  dividends?: number;
}

interface StockMetadata {
  ticker: string;
  firstTradeDate?: string;
  lastUpdate: string;
  dataSource: 'eod' | 'yahoo';
  recordCount: number;
}
```

### 5. CLI Commands

#### `stocker fetch <ticker>`
- Fetch historical data for a ticker
- Options:
  - `--start`: Start date (default: first available)
  - `--end`: End date (default: today)
  - `--source`: Data source (eod/yahoo, default: eod)
  - `--update`: Only fetch missing/recent data
  - `--parallel`: Number of parallel fetches (default: 5)

#### `stocker list`
- List all tickers in the local store
- Shows last update date and record count

#### `stocker info <ticker>`
- Show metadata about a ticker
- First/last date, record count, data gaps

#### `stocker commit <message>`
- Git commit all data changes
- Auto-generates commit message if not provided

#### `stocker import <directory>`
- Import existing CSV historical data
- Converts to Parquet format

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)
1. ✅ Set up TypeScript project with Bun
2. ✅ Implement configuration management
3. ✅ Create DuckDB storage provider interface
4. ✅ Set up basic CLI structure with Clipanion

### Phase 2: Data Services (Week 2)
1. Port EOD Historical Data service
2. Port Yahoo Finance service
3. Implement rate limiting and retry logic
4. Create unified data transformation layer

### Phase 3: Storage Implementation (Week 3)
1. Implement DuckDB storage provider
2. Create Parquet read/write functionality
3. Implement merge/update logic
4. Set up database views and indexes

### Phase 4: CLI Commands (Week 4)
1. Implement fetch command
2. Implement update command
3. Implement query interface
4. Add list, info, and import commands

### Phase 5: Polish & Testing (Week 5)
1. Add comprehensive error handling
2. Implement progress bars and logging
3. Write integration tests
4. Create documentation

## Migration Strategy

### From Existing Python Project
1. **Data Migration**:
   - Create `import` command to read existing CSV files
   - Convert to Parquet format preserving all fields
   - Maintain data integrity and historical accuracy

2. **Configuration Migration**:
   - Support same environment variables
   - Compatible YAML configuration format

## Future Enhancements (Out of Scope for V1)
- Intraday data support (minute/hourly bars)
- Technical analysis modules (peaks, volatility, etc.)
- IPO tracking functionality
- Multiple data source aggregation
- Real-time data streaming
- Web UI for data visualization

## Success Criteria
1. Successfully fetch and store daily OHLCV data for any ticker
2. Query performance: < 100ms for single ticker yearly data
3. Storage efficiency: > 70% size reduction vs CSV
4. Git-friendly: Incremental updates create small diffs
5. Zero data loss during updates/merges

## Configuration Example
```yaml
# ~/.stocker/config.yml
sources:
  eod:
    apiKey: ${EOD_HISTORICAL_DATA_API_KEY}
    rateLimit: 20  # requests per second
  yahoo:
    enabled: true
    
storage:
  dataDir: ~/.stocker/data
  
defaults:
  startDate: '2020-01-01'
  parallel: 5
```

## Error Handling
- Network failures: Exponential backoff retry
- API limits: Respect rate limits, queue requests
- Data conflicts: Prefer newer data, log conflicts
- Storage failures: Transaction rollback, maintain consistency

## Security Considerations
- API keys stored in environment variables or config file
- No keys in git repository
- Local data storage only (no cloud sync in V1)

## Dependencies
```json
{
  "dependencies": {
    "clipanion": "^4.0.0",
    "duckdb": "^1.0.0",
    "parquetjs": "^1.0.0",
    "simple-git": "^3.0.0",
    "axios": "^1.0.0",
    "p-limit": "^5.0.0",
    "yaml": "^2.0.0"
  }
}
```