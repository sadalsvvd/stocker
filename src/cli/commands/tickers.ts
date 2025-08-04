import { Command, Option } from "clipanion";
import { Stocker } from "../../index";
import { SECService } from "../../services/sec";
import { ReferenceStorage } from "../../storage/reference";
import { TickerUpdaterService } from "../../services/ticker-updater";

export class TickersCommand extends Command {
  static override paths = [["tickers"]];

  static override usage = Command.Usage({
    description: "Manage ticker universe",
    details: `
      This command manages the ticker universe, including:
      - Updating from SEC data
      - Searching for tickers
      - Showing ticker statistics
      
      Use one of the subcommands listed below.
    `,
  });

  async execute() {
    // When no subcommand is provided, show available subcommands
    console.log("Usage: stocker tickers <subcommand>\n");
    console.log("Available subcommands:");
    console.log("  update              Update ticker list from SEC");
    console.log("  search <query>      Search for tickers");  
    console.log("  stats               Show ticker universe statistics");
    console.log("\nRun 'stocker tickers <subcommand> --help' for more information on a subcommand.");
    return 0;
  }
}

export class TickersUpdateCommand extends Command {
  static override paths = [["tickers", "update"]];

  static override usage = Command.Usage({
    description: "Update ticker list from SEC and other sources",
    details: `
      This command fetches the latest ticker information from:
      - SEC company tickers (all US public companies)
      - Finviz for additional metadata
      - Detects delisted tickers
      - Updates data quality ratings
    `,
    examples: [
      ["Update ticker universe", "stocker tickers update"],
      ["Update and verify quality", "stocker tickers update --verify"],
    ],
  });
  
  verify = Option.Boolean("--verify", false, {
    description: "Verify and update data quality ratings",
  });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    try {
      // Use the unified updater with multiple sources
      const dataDir = stocker.storage.dataDir.replace("/stocks", "");
      const refStorage = new ReferenceStorage(dataDir);
      await refStorage.init();
      
      // Get API key from config if available
      let eodApiKey: string | undefined;
      try {
        const configPath = stocker['configPath'] || '~/.stocker/config.yml';
        const { loadConfig } = await import('../../config');
        const config = await loadConfig(configPath);
        eodApiKey = config.sources?.eodhd?.apiKey;
      } catch (e) {
        // Config may not exist, continue without API key
      }
      
      const updater = new TickerUpdaterService(refStorage, eodApiKey);
      
      console.log('Starting ticker universe update...');
      const result = await updater.updateTickerUniverse();
      
      console.log('\n=== Update Summary ===');
      console.log(`Added: ${result.added} new tickers`);
      console.log(`Updated: ${result.updated} existing tickers`);
      console.log(`Delisted: ${result.delisted} tickers`);
      
      if (result.errors.length > 0) {
        console.log(`\nErrors: ${result.errors.length}`);
        result.errors.slice(0, 5).forEach(err => console.log(`  - ${err}`));
      }
      
      if (this.verify) {
        console.log('\nVerifying data quality...');
        await updater.verifyAndRateTickers();
      }
      
      const stats = await refStorage.getTickerStats();
      console.log('\n=== Database Stats ===');
      console.log(`Total tickers: ${stats.total}`);
      console.log(`Active: ${stats.active}`);
      console.log(`Delisted: ${stats.delisted}`);
      console.log('\nBy exchange:');
      Object.entries(stats.byExchange).forEach(([exchange, count]) => {
        console.log(`  ${exchange}: ${count}`);
      });
      
      await refStorage.close();
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.error(`Failed to update tickers: ${errorMessage}`);
    }
  }
}

export class TickersSearchCommand extends Command {
  static override paths = [["tickers", "search"]];

  static override usage = Command.Usage({
    description: "Search for tickers",
    details: `
      Search for tickers by symbol or company name.
      Returns up to 50 matches ordered by relevance.
    `,
    examples: [
      ["Search for Apple", "stocker tickers search AAPL"],
      ["Search by company name", "stocker tickers search Tesla"],
    ],
  });

  query = Option.String({ required: true });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    const referenceStorage = new ReferenceStorage(
      stocker.storage.dataDir.replace("/stocks", "")
    );
    await referenceStorage.init();

    try {
      const results = await referenceStorage.searchTickers(this.query);

      if (results.length === 0) {
        console.log(`No tickers found matching "${this.query}"`);
        return;
      }

      console.log(`Found ${results.length} ticker(s):\n`);

      results.forEach((ticker) => {
        const status = ticker.status === "active" ? "" : ` [${ticker.status}]`;
        console.log(
          `${ticker.symbol.padEnd(10)} ${ticker.companyName}${status}`
        );
        if (ticker.sector) {
          console.log(
            `           ${ticker.sector}${
              ticker.industry ? ` - ${ticker.industry}` : ""
            }`
          );
        }
      });
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.error(`Search failed: ${errorMessage}`);
    }
  }
}

export class TickersStatsCommand extends Command {
  static override paths = [["tickers", "stats"]];

  static override usage = Command.Usage({
    description: "Show ticker universe statistics",
    examples: [["Show ticker stats", "stocker tickers stats"]],
  });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    const referenceStorage = new ReferenceStorage(
      stocker.storage.dataDir.replace("/stocks", "")
    );
    await referenceStorage.init();

    try {
      const stats = await referenceStorage.getTickerStats();

      console.log("Ticker Universe Statistics:");
      console.log(`  Total tickers: ${stats.total}`);
      console.log(`  Active: ${stats.active}`);
      console.log(`  Delisted: ${stats.delisted}`);

      if (Object.keys(stats.byExchange).length > 0) {
        console.log("\nBy exchange:");
        Object.entries(stats.byExchange).forEach(([exchange, count]) => {
          console.log(`  ${exchange}: ${count}`);
        });
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.error(`Failed to get stats: ${errorMessage}`);
    }
  }
}
