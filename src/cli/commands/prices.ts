import { Command, Option } from "clipanion";
import { loadPrices, getLatestPrice, getFirstPrice, PriceLoader } from "../../services/price-loader.js";

// Custom JSON stringifier to handle BigInt
function jsonStringify(obj: any): string {
  return JSON.stringify(obj, (key, value) =>
    typeof value === 'bigint' ? Number(value) : value
  );
}

export class PricesCommand extends Command {
  static paths = [["prices"]];
  static usage = Command.Usage({
    description: "Load and display price data for a symbol",
    examples: [
      ["Get all prices for AAPL", "$0 prices AAPL"],
      ["Get latest price for AAPL", "$0 prices AAPL --latest"],
      ["Get first price for AAPL", "$0 prices AAPL --first"],
      ["Get prices for date range", "$0 prices AAPL --from 2024-01-01 --to 2024-01-31"],
    ],
  });

  symbol = Option.String({ required: true });
  latest = Option.Boolean("--latest", false, {
    description: "Show only the latest price",
  });
  first = Option.Boolean("--first", false, {
    description: "Show only the first price",
  });
  from = Option.String("--from", {
    description: "Start date (YYYY-MM-DD)",
  });
  to = Option.String("--to", {
    description: "End date (YYYY-MM-DD)",
  });

  async execute() {
    const dataDir = process.env.STOCKER_DATA_DIR || "./data";
    
    if (this.latest) {
      const price = await getLatestPrice(this.symbol);
      console.log(jsonStringify(price, null, 2));
      return;
    }
    
    if (this.first) {
      const price = await getFirstPrice(this.symbol);
      console.log(jsonStringify(price, null, 2));
      return;
    }
    
    if (this.from || this.to) {
      const loader = new PriceLoader(dataDir);
      await loader.init();
      
      const from = this.from || "1900-01-01";
      const to = this.to || new Date().toISOString().split('T')[0];
      
      const prices = await loader.loadPricesRange(this.symbol, from, to);
      console.log(jsonStringify(prices, null, 2));
      
      await loader.close();
      return;
    }
    
    // Default: load all prices
    const prices = await loadPrices(this.symbol);
    console.log(jsonStringify(prices, null, 2));
  }
}