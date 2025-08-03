export interface OHLCV {
  date: string; // YYYY-MM-DD format
  
  // Raw prices (as reported on that day)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  
  // Adjusted prices (for splits/dividends)
  adjOpen: number;
  adjHigh: number;
  adjLow: number;
  adjClose: number;
  adjVolume: number;
  
  // Metadata
  percentChange?: number;
  split?: boolean;
  dividends?: number;
  dataSource?: string;
  fetchedAt?: string;
  
  // Legacy - will be removed
  adjustedClose?: number;
}

export interface StockMetadata {
  ticker: string;
  firstTradeDate?: string;
  lastUpdate: string;
  dataSource: "eod" | "yahoo";
  recordCount: number;
}

export interface DataService {
  fetchDaily(
    ticker: string,
    startDate?: string,
    endDate?: string
  ): Promise<OHLCV[]>;
  fetchSplits?(ticker: string): Promise<SplitData[]>;
}

export interface SplitData {
  date: string;
  ratio: string;
}

export interface Config {
  sources: {
    eodhd?: {
      apiKey: string;
      rateLimit?: number;
    };
  };
  storage?: {
    dataDir: string;
  };
  defaults?: {
    startDate: string;
    parallel: number;
  };
}
