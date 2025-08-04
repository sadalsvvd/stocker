import axios from "axios";
import type { TickerInfo } from "../types/index";

interface SECCompanyTicker {
  cik_str: number;
  ticker: string;
  title: string;
}

interface SECTickersResponse {
  [key: string]: SECCompanyTicker;
}

export class SECService {
  private readonly baseUrl = "https://www.sec.gov";
  private readonly userAgent: string;

  constructor() {
    // SEC requires a user agent with contact info
    this.userAgent = "Stocker/1.0 (contact@example.com)";
  }

  /**
   * Fetch all current US public company tickers from SEC
   * Updated daily by SEC
   */
  async fetchAllTickers(): Promise<TickerInfo[]> {
    try {
      console.log("Fetching tickers from SEC...");
      
      const response = await axios.get<SECTickersResponse>(
        `${this.baseUrl}/files/company_tickers.json`,
        {
          headers: {
            "User-Agent": this.userAgent,
            "Accept": "application/json",
          },
          timeout: 30000,
        }
      );

      // Convert SEC format to our TickerInfo format
      const tickers: TickerInfo[] = Object.values(response.data).map((company) => ({
        symbol: company.ticker,
        companyName: company.title,
        exchange: "US", // SEC doesn't specify exchange, we'll need to enrich this
        status: "active" as const,
        firstSeen: new Date().toISOString().split("T")[0]!,
        lastUpdated: new Date().toISOString().split("T")[0]!,
        metadata: {
          cik: company.cik_str.toString().padStart(10, "0"), // CIK is padded to 10 digits
          source: "SEC",
        },
      }));

      console.log(`Fetched ${tickers.length} tickers from SEC`);
      return tickers;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        throw new Error(`Failed to fetch SEC tickers: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Search for recent S-1 filings (IPO registrations)
   * This would need more complex implementation for production use
   */
  async searchRecentIPOFilings(days: number = 30): Promise<any[]> {
    // This is a placeholder - implementing full EDGAR search is complex
    // For production, you'd want to:
    // 1. Use the EDGAR full-text search API
    // 2. Parse XBRL/XML filings
    // 3. Extract relevant IPO information
    
    console.log(`Searching for S-1 filings from last ${days} days...`);
    
    // For now, return empty array
    // Full implementation would require EDGAR API integration
    return [];
  }

  /**
   * Get company details by CIK
   */
  async getCompanyInfo(cik: string): Promise<any> {
    try {
      const paddedCik = cik.padStart(10, "0");
      const response = await axios.get(
        `${this.baseUrl}/Archives/edgar/data/${paddedCik}/company.json`,
        {
          headers: {
            "User-Agent": this.userAgent,
            "Accept": "application/json",
          },
        }
      );
      
      return response.data;
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }
}