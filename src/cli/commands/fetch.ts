import { Command, Option } from 'clipanion';
import { Stocker } from '../../index';
import { format } from 'date-fns';
import * as t from 'typanion';

export class FetchCommand extends Command {
  static override paths = [['fetch']];

  static override usage = Command.Usage({
    description: 'Fetch historical data for a ticker',
    details: `
      This command fetches historical stock data for the specified ticker symbol.
      
      By default, it fetches all available data for the ticker. You can use the
      --start and --end options to specify a date range, or --update to only
      fetch recent data since the last update.
    `,
    examples: [
      ['Fetch all data for AAPL', 'stocker fetch AAPL'],
      ['Fetch data from 2023 onwards', 'stocker fetch AAPL --start 2023-01-01'],
      ['Update with recent data only', 'stocker fetch AAPL --update'],
      ['Fetch multiple tickers in parallel', 'stocker fetch AAPL MSFT GOOGL --parallel 3'],
    ],
  });

  ticker = Option.Rest({ required: 1 });

  start = Option.String('--start', {
    description: 'Start date (YYYY-MM-DD)',
  });

  end = Option.String('--end', {
    description: 'End date (YYYY-MM-DD)',
  });

  source = Option.String('--source', 'eod', {
    description: 'Data source (eod/yahoo)',
    validator: t.isEnum(['eod', 'yahoo']),
  });

  update = Option.Boolean('--update', false, {
    description: 'Only fetch missing/recent data',
  });

  parallel = Option.String('--parallel', '5', {
    description: 'Number of parallel fetches',
  });

  force = Option.Boolean('--force', false, {
    description: 'Force fetch even if data exists',
  });

  verbose = Option.Boolean('--verbose,-v', false, {
    description: 'Show detailed progress',
  });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    const tickers = this.ticker;
    const parallelLimit = parseInt(this.parallel, 10);

    if (this.verbose) {
      console.log(`Fetching ${tickers.length} ticker(s) with parallel limit: ${parallelLimit}`);
    }

    for (const ticker of tickers) {
      try {
        const normalizedTicker = ticker.toUpperCase();
        
        if (this.verbose) {
          console.log(`\nFetching ${normalizedTicker}...`);
        }

        const exists = await stocker.storage.exists(normalizedTicker);
        
        if (exists && !this.update && !this.force) {
          const metadata = await stocker.storage.getMetadata(normalizedTicker);
          if (metadata) {
            console.log(`${normalizedTicker}: Already exists (last update: ${metadata.lastUpdate}). Use --update or --force to fetch.`);
            continue;
          }
        }

        let startDate = this.start;
        let endDate = this.end;

        if (this.update && exists) {
          const lastUpdate = await stocker.storage.getLastUpdate(normalizedTicker);
          if (lastUpdate) {
            startDate = format(new Date(lastUpdate.getTime() + 24 * 60 * 60 * 1000), 'yyyy-MM-dd');
            if (this.verbose) {
              console.log(`  Updating from ${startDate}`);
            }
          }
        }

        const options = {
          start: startDate,
          end: endDate,
          update: this.update,
        };

        await stocker.fetch(normalizedTicker, options);
        
        const metadata = await stocker.storage.getMetadata(normalizedTicker);
        if (metadata) {
          console.log(`${normalizedTicker}: Successfully fetched (${metadata.recordCount} records, ${metadata.firstTradeDate} to ${metadata.lastUpdate})`);
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error(`${ticker}: Failed - ${errorMessage}`);
        if (this.verbose && error instanceof Error && error.stack) {
          console.error(error.stack);
        }
      }
    }

    await stocker.close();
  }
}