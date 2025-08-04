import { Command, Option } from "clipanion";
import { readFileSync } from "fs";
import { parse } from "csv-parse/sync";
import { ReferenceStorage } from "../../storage/reference.js";
import { Stocker } from "../../index.js";

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
  static paths = [["migrate-csv"]];
  static usage = Command.Usage({
    description: "Migrate first_trade_dates_master.csv to reference storage",
    examples: [
      ["Import CSV data", "$0 migrate-csv data/first_trade_dates_master.csv"],
      [
        "Import and update existing",
        "$0 migrate-csv data/first_trade_dates_master.csv --update",
      ],
    ],
  });

  csvPath = Option.String({ required: true });
  update = Option.Boolean("--update", false, {
    description: "Update existing tickers instead of skipping",
  });
  dryRun = Option.Boolean("--dry-run", false, {
    description: "Show what would be imported without actually importing",
  });

  async execute(): Promise<number> {
    try {
      console.log(`Reading CSV from ${this.csvPath}...`);

      // Read and parse CSV
      const csvContent = readFileSync(this.csvPath, "utf-8");
      const rows: CSVRow[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
      });

      console.log(`Found ${rows.length} rows in CSV`);

      // Initialize storage
      const stocker = new Stocker();
      await stocker.init();
      const refStorage = new ReferenceStorage(stocker["storage"].dataDir);
      await refStorage.init();

      // Use the built-in CSV import method
      const result = await refStorage.importFromCSV(rows);

      // If dry run, just show what would be imported
      if (this.dryRun) {
        console.log(`Would import ${result.imported} tickers`);
        console.log(`Would skip ${result.skipped} delisted entries`);
        if (result.errors.length > 0) {
          console.log(`\nPotential errors:`);
          result.errors
            .slice(0, 10)
            .forEach((err) => console.log(`  - ${err}`));
        }
        return 0;
      }

      // Print summary
      console.log("\n=== Migration Summary ===");
      console.log(`Total rows: ${rows.length}`);
      console.log(`Imported: ${result.imported}`);
      console.log(`Skipped: ${result.skipped}`);
      console.log(`Errors: ${result.errors.length}`);

      if (result.errors.length > 0) {
        console.log("\nErrors:");
        result.errors.slice(0, 10).forEach((err) => console.log(`  - ${err}`));
        if (result.errors.length > 10) {
          console.log(`  ... and ${result.errors.length - 10} more`);
        }
      }

      // Show data quality summary
      if (result.imported > 0) {
        const stats = await refStorage.getTickerStats();
        console.log("\n=== Database Stats ===");
        console.log(`Total tickers: ${stats.total}`);
        console.log(`Active: ${stats.active}`);
        console.log(`Delisted: ${stats.delisted}`);
        console.log("\nBy Exchange:");
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
      console.error("Migration failed:", error);
      return 1;
    }
  }
}
