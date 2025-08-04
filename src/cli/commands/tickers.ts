import { Command, Option } from "clipanion";
import { Stocker } from "../../index";
import { SECService } from "../../services/sec";
import { ReferenceStorage } from "../../storage/reference";

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
      - Future: NASDAQ FTP, IPO calendars, etc.
    `,
    examples: [["Update ticker universe", "stocker tickers update"]],
  });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    const referenceStorage = new ReferenceStorage(
      stocker.storage.dataDir.replace("/stocks", "")
    );
    await referenceStorage.init();

    try {
      // Fetch from SEC
      const secService = new SECService();
      console.log("Fetching ticker data from SEC...");
      const secTickers = await secService.fetchAllTickers();

      // Get existing tickers to preserve metadata
      const existingTickers = await referenceStorage.getAllTickers();
      const existingMap = new Map(existingTickers.map((t) => [t.symbol, t]));

      // Merge with existing data
      const mergedTickers = secTickers.map((ticker) => {
        const existing = existingMap.get(ticker.symbol);
        if (existing) {
          // Preserve certain fields from existing data
          return {
            ...ticker,
            firstSeen: existing.firstSeen,
            ipoDate: existing.ipoDate,
            sector: existing.sector || ticker.sector,
            industry: existing.industry || ticker.industry,
            metadata: {
              ...existing.metadata,
              ...ticker.metadata,
            },
          };
        }
        return ticker;
      });

      // Save to database
      await referenceStorage.upsertTickers(mergedTickers);

      // Show stats
      const stats = await referenceStorage.getTickerStats();
      console.log("\nTicker universe updated:");
      console.log(`  Total tickers: ${stats.total}`);
      console.log(`  Active: ${stats.active}`);
      console.log(`  Delisted: ${stats.delisted}`);
      console.log("\nBy exchange:");
      Object.entries(stats.byExchange).forEach(([exchange, count]) => {
        console.log(`  ${exchange}: ${count}`);
      });
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
