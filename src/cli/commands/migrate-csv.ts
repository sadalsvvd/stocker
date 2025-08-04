import { Command, Option } from 'clipanion';
import { readFileSync } from 'fs';
import { parse } from 'csv-parse/sync';
import { ReferenceStorage } from '../../storage/reference.js';
import { Stocker } from '../../index.js';
import type { TickerInfo } from '../../types/index.js';

interface CSVRow {
  symbol: string;
  security_type: string;
  source_rating: string;
  datetime: string;
  company: string;
  lat: string;
  lng: string;
  sector: string;
  industry: string;
  country: string;
  volume: string;
  exchange: string;
  location_rating: string;
  source_note: string;
}

// NYC default coordinates
const NYC_LAT = 40.7069;
const NYC_LNG = -74.0113;

export class MigrateCSVCommand extends Command {
  static paths = [['migrate-csv']];
  static usage = Command.Usage({
    description: 'Migrate first_trade_dates_master.csv to reference storage',
    examples: [
      ['Import CSV data', '$0 migrate-csv data/first_trade_dates_master.csv'],
      ['Import and update existing', '$0 migrate-csv data/first_trade_dates_master.csv --update'],
    ],
  });

  csvPath = Option.String({ required: true });
  update = Option.Boolean('--update', false, {
    description: 'Update existing tickers instead of skipping',
  });
  dryRun = Option.Boolean('--dry-run', false, {
    description: 'Show what would be imported without actually importing',
  });

  async execute(): Promise<number> {
    try {
      console.log(`Reading CSV from ${this.csvPath}...`);
      
      // Read and parse CSV
      const csvContent = readFileSync(this.csvPath, 'utf-8');
      const rows: CSVRow[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
      });

      console.log(`Found ${rows.length} rows in CSV`);

      // Initialize storage
      const stocker = new Stocker();
      await stocker.init();
      const refStorage = new ReferenceStorage(stocker['storage'].dataDir);
      await refStorage.init();

      // Process rows
      let imported = 0;
      let skipped = 0;
      let updated = 0;
      const errors: string[] = [];
      const tickers: TickerInfo[] = [];

      for (const row of rows) {
        try {
          // Skip delisted entries (prefixed with #)
          if (row.symbol?.startsWith('#')) {
            skipped++;
            continue;
          }

          // Check if ticker exists
          const existing = await refStorage.getTicker(row.symbol);
          if (existing && !this.update) {
            skipped++;
            continue;
          }

          // Parse coordinates
          let lat = NYC_LAT;
          let lng = NYC_LNG;
          
          if (row.lat && row.lat !== 'NY_LAT') {
            lat = parseFloat(row.lat) || NYC_LAT;
          }
          if (row.lng && row.lng !== 'NY_LNG') {
            lng = parseFloat(row.lng) || NYC_LNG;
          }

          // Convert to TickerInfo with enhanced metadata
          const ticker: TickerInfo = {
            symbol: row.symbol,
            companyName: row.company || row.symbol,
            exchange: row.exchange || 'UNKNOWN',
            status: 'active',
            firstSeen: row.datetime || new Date().toISOString().split('T')[0],
            lastUpdated: new Date().toISOString().split('T')[0],
            
            // Map sector/industry
            sector: row.sector || undefined,
            industry: row.industry || undefined,
            
            // Store additional data in metadata
            metadata: {
              // Location data
              lat,
              lng,
              timezone: 'America/New_York',
              
              // Data quality
              sourceRating: row.source_rating,
              sourceNote: row.source_note,
              
              // Trading data
              firstTradeDate: row.datetime,
              volume: row.volume,
              
              // Other metadata
              country: row.country,
              securityType: row.security_type || 'stocks',
              locationRating: row.location_rating,
              
              // Mark as imported from CSV
              importedFrom: 'first_trade_dates_master.csv',
              importedAt: new Date().toISOString(),
            }
          };

          if (this.dryRun) {
            console.log(`Would import: ${ticker.symbol} - ${ticker.companyName}`);
          } else {
            tickers.push(ticker);
          }

          if (existing) {
            updated++;
          } else {
            imported++;
          }
        } catch (error) {
          errors.push(`Failed to process ${row.symbol}: ${error}`);
        }
      }

      // Bulk insert if not dry run
      if (!this.dryRun && tickers.length > 0) {
        console.log(`Importing ${tickers.length} tickers...`);
        await refStorage.upsertTickers(tickers);
      }

      // Print summary
      console.log('\n=== Migration Summary ===');
      console.log(`Total rows: ${rows.length}`);
      console.log(`Imported: ${imported}`);
      console.log(`Updated: ${updated}`);
      console.log(`Skipped: ${skipped}`);
      console.log(`Errors: ${errors.length}`);

      if (errors.length > 0) {
        console.log('\nErrors:');
        errors.slice(0, 10).forEach(err => console.log(`  - ${err}`));
        if (errors.length > 10) {
          console.log(`  ... and ${errors.length - 10} more`);
        }
      }

      // Show data quality summary
      if (!this.dryRun && tickers.length > 0) {
        const stats = await refStorage.getTickerStats();
        console.log('\n=== Database Stats ===');
        console.log(`Total tickers: ${stats.total}`);
        console.log(`Active: ${stats.active}`);
        console.log(`Delisted: ${stats.delisted}`);
        console.log('\nBy Exchange:');
        Object.entries(stats.byExchange)
          .sort(([, a], [, b]) => b - a)
          .slice(0, 10)
          .forEach(([exchange, count]) => {
            console.log(`  ${exchange}: ${count}`);
          });
      }

      await refStorage.close();
      return 0;
    } catch (error) {
      console.error('Migration failed:', error);
      return 1;
    }
  }
}

// Helper to extract delisted tickers from CSV
export async function extractDelistedTickers(csvPath: string): Promise<{
  symbol: string;
  delistedDate: string;
  reason: string;
}[]> {
  const csvContent = readFileSync(csvPath, 'utf-8');
  const rows: CSVRow[] = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
  });

  const delisted: { symbol: string; delistedDate: string; reason: string; }[] = [];

  for (const row of rows) {
    if (row.symbol?.startsWith('#')) {
      // Parse delisted format: #_SYMBOL_DELISTED_AS_OF_DATE or #_SYMBOL_NAME_CHANGE_AS_OF_DATE
      const parts = row.symbol.split('_');
      if (parts.length >= 5) {
        const symbol = parts[1];
        const reason = parts[2]; // DELISTED or NAME
        const date = parts[parts.length - 1];
        
        delisted.push({
          symbol,
          delistedDate: date,
          reason: reason === 'NAME' ? 'name_change' : 'delisted'
        });
      }
    }
  }

  return delisted;
}