# Stocker

A TypeScript-based CLI tool for fetching, storing, and managing historical stock market data using DuckDB and Parquet files.

## Features

- Fetch historical stock data from EOD Historical Data API
- Store data efficiently in Parquet format
- Query data using DuckDB's SQL interface
- Git-friendly data storage (commit your data!)
- TypeScript with full type safety
- Built on Bun for maximum performance

## Installation

```bash
bun install
```

## Configuration

Set your API key as an environment variable:

```bash
export EODHD_API_KEY=your_api_key_here
```

Or create a config file at `~/.stocker/config.yml`:

```yaml
sources:
  eodhd:
    apiKey: your_api_key_here
    rateLimit: 20
    
storage:
  dataDir: ./data  # Defaults to project data/ directory
  
defaults:
  startDate: '2020-01-01'
  parallel: 5
```

## Usage

### Programmatic API

```typescript
import { Stocker } from './src/index.ts';

const stocker = new Stocker({
  sources: {
    eodhd: {
      apiKey: 'your_api_key'
    }
  }
});

await stocker.init();

// Fetch data
await stocker.fetch('AAPL', {
  start: '2024-01-01',
  end: '2024-01-31'
});

// Update with latest data
await stocker.fetch('AAPL', { update: true });

// List stored tickers
const tickers = await stocker.list();

// Get info about a ticker
await stocker.info('AAPL');
```

### Data Storage

Data is stored in the following structure:

```
data/
├── catalog.duckdb      # DuckDB metadata (gitignored)
└── stocks/
    ├── AAPL/
    │   └── daily.parquet
    ├── MSFT/
    │   └── daily.parquet
    └── [ticker]/
        └── daily.parquet
```

### Querying Data

The DuckDB storage provider creates a `daily` view for easy querying:

```sql
SELECT * FROM daily 
WHERE ticker = 'AAPL' 
  AND date >= '2024-01-01'
ORDER BY date;
```

## Development

Run tests:
```bash
bun test-stocker.ts
```

## Next Steps

- [ ] Add Clipanion CLI interface
- [ ] Implement direct SQL query interface
- [ ] Add data validation and integrity checks
- [ ] Support for intraday data
- [ ] Add technical analysis modules

## License

MIT
