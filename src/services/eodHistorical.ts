import axios from "axios";
import type { OHLCV, SplitData, DataService } from "../types/index.ts";
import { getConfig } from "../config.ts";

export class EODHistoricalService implements DataService {
  private apiKey: string;
  private baseUrl = "https://eodhistoricaldata.com/api";

  constructor(apiKey?: string) {
    this.apiKey = apiKey || getConfig().sources.eodhd?.apiKey || "";
    if (!this.apiKey) {
      throw new Error("EOD Historical Data API key is required");
    }
  }

  async fetchDaily(
    ticker: string,
    startDate?: string,
    endDate?: string
  ): Promise<OHLCV[]> {
    try {
      const params: Record<string, string> = {
        api_token: this.apiKey,
        fmt: "json",
      };

      if (startDate) params.from = startDate;
      if (endDate) params.to = endDate;

      const response = await axios.get(`${this.baseUrl}/eod/${ticker}`, {
        params,
      });

      if (!response.data || !Array.isArray(response.data)) {
        throw new Error(`Invalid response for ticker ${ticker}`);
      }

      return response.data.map(transformEODData);
    } catch (error) {
      if (axios.isAxiosError(error)) {
        if (error.response?.status === 404) {
          throw new Error(`Ticker ${ticker} not found`);
        }
        throw new Error(
          `EOD API error: ${error.response?.status} - ${error.response?.statusText}`
        );
      }
      throw error;
    }
  }

  async fetchSplits(ticker: string): Promise<SplitData[]> {
    try {
      const params = {
        api_token: this.apiKey,
        fmt: "json",
      };

      const response = await axios.get(`${this.baseUrl}/splits/${ticker}`, {
        params,
      });

      if (!response.data || !Array.isArray(response.data)) {
        return [];
      }

      return response.data.map((split: any) => ({
        date: split.date,
        ratio: split.split,
      }));
    } catch (error) {
      console.warn(`Failed to fetch splits for ${ticker}:`, error);
      return [];
    }
  }

}

// Utility function to transform EOD Historical data
function transformEODData(row: any): OHLCV {
  // EODHD provides adjusted_close, we need to calculate adjustment factor
  const rawClose = Number(row.close);
  const adjClose = Number(row.adjusted_close || row.close);
  const adjFactor = rawClose > 0 ? adjClose / rawClose : 1;
  
  // Calculate all adjusted values
  const rawOpen = Number(row.open);
  const rawHigh = Number(row.high);
  const rawLow = Number(row.low);
  const rawVolume = Number(row.volume) || 0;
  
  return {
    date: row.date,
    
    // Raw prices (as traded on that day)
    open: rawOpen,
    high: rawHigh,
    low: rawLow,
    close: rawClose,
    volume: rawVolume,
    
    // Adjusted prices (for splits/dividends)
    adjOpen: rawOpen * adjFactor,
    adjHigh: rawHigh * adjFactor,
    adjLow: rawLow * adjFactor,
    adjClose: adjClose,
    adjVolume: adjFactor !== 1 ? Math.round(rawVolume / adjFactor) : rawVolume,
    
    // Metadata
    percentChange: row.change_p || 0, // EODHD provides this
    dataSource: 'eod',
    fetchedAt: new Date().toISOString(),
    
    // Legacy field for backward compatibility
    adjustedClose: adjClose,
  };
}
