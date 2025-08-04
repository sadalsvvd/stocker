import axios from "axios";
import type { AxiosInstance } from "axios";
import * as cheerio from "cheerio";
import type { TickerInfo } from "../types";

export class FinvizFetcher {
  private axios: AxiosInstance;
  private cache: Map<string, TickerInfo>;
  private lastRequestTime: number = 0;
  private minRequestDelay: number = 1000; // 1 second between requests

  constructor() {
    this.axios = axios.create({
      baseURL: "https://finviz.com",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        Connection: "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
      },
      timeout: 10000,
    });
    this.cache = new Map();
  }

  private async enforceRateLimit(): Promise<void> {
    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequestTime;
    if (timeSinceLastRequest < this.minRequestDelay) {
      await new Promise((resolve) =>
        setTimeout(resolve, this.minRequestDelay - timeSinceLastRequest)
      );
    }
    this.lastRequestTime = Date.now();
  }

  async getTickerInfo(symbol: string): Promise<TickerInfo | null> {
    // Check cache first
    if (this.cache.has(symbol)) {
      return this.cache.get(symbol)!;
    }

    try {
      await this.enforceRateLimit();

      const response = await this.axios.get(`/quote.ashx?t=${symbol}`);
      const $ = cheerio.load(response.data);

      // Extract all data fields
      const dataFields: Record<string, string> = {};
      $(".snapshot-table2 tr").each((_, row) => {
        const $row = $(row);
        $row.find("td").each((i, cell) => {
          const $cell = $(cell);
          if (i % 2 === 0) {
            const label = $cell.text().trim();
            const $nextCell = $cell.next();
            if ($nextCell.length > 0) {
              const value = $nextCell.text().trim();
              if (label && value) {
                dataFields[label] = value;
              }
            }
          }
        });
      });

      // Extract company name (multiple attempts)
      let companyName = $(".fullview-title .tab-link").text().trim();
      if (!companyName) {
        const pageTitle = $("title").text();
        const titleMatch = pageTitle.match(/^([^-]+) - /);
        if (titleMatch && titleMatch[1]) {
          companyName = titleMatch[1].trim();
        }
      }

      // If still just the symbol, look in profile
      if (!companyName || companyName === symbol) {
        const profileText = $(".fullview-profile").text();
        const profileMatch = profileText.match(/^([^,.]+)/);
        if (profileMatch && profileMatch[1]) {
          companyName = profileMatch[1].trim();
        }
      }

      // Extract exchange
      const exchangeText = $('a[href*="screener.ashx?v=111&f=exch_"]')
        .text()
        .trim();

      // Extract sector and industry
      const sector = $('a[href*="screener.ashx?v=111&f=sec_"]').text().trim();
      const industry = $('a[href*="screener.ashx?v=111&f=ind_"]').text().trim();

      // Parse IPO date
      let ipoDate: string | undefined;
      if (dataFields["IPO"]) {
        // Convert "Dec 12, 1980" to ISO format
        const date = new Date(dataFields["IPO"]);
        if (!isNaN(date.getTime())) {
          ipoDate = date.toISOString().split("T")[0];
        }
      }

      const tickerInfo: TickerInfo = {
        symbol: symbol.toUpperCase(),
        companyName: companyName || symbol,
        exchange: this.normalizeExchange(exchangeText),
        sector: sector || undefined,
        industry: industry || undefined,
        ipoDate: ipoDate,
        status: "active", // FinViz only shows active stocks
        firstSeen: new Date().toISOString(),
        lastUpdated: new Date().toISOString(),
        metadata: {
          marketCap: dataFields["Market Cap"],
          country: dataFields["Country"],
          employees: dataFields["Employees"],
          index: dataFields["Index"],
        },
      };

      this.cache.set(symbol, tickerInfo);
      return tickerInfo;
    } catch (error: any) {
      if (error.response?.status === 404) {
        // Stock not found
        return null;
      }
      if (error.response?.status === 403) {
        throw new Error(
          "Blocked by Cloudflare. Consider reducing request rate."
        );
      }
      throw error;
    }
  }

  async getMultipleTickers(
    symbols: string[]
  ): Promise<Map<string, TickerInfo | null>> {
    const results = new Map<string, TickerInfo | null>();

    for (const symbol of symbols) {
      try {
        const info = await this.getTickerInfo(symbol);
        results.set(symbol, info);
      } catch (error) {
        console.error(`Error fetching ${symbol}:`, error);
        results.set(symbol, null);
      }
    }

    return results;
  }

  async getAllTickersFromScreener(filters?: {
    sector?: string;
    exchange?: string;
    marketCap?: string;
  }): Promise<string[]> {
    await this.enforceRateLimit();

    const params: Record<string, string> = {
      v: "111", // Table view
    };

    // Build filter string
    const filterParts: string[] = [];
    if (filters?.sector) {
      filterParts.push(`sec_${this.normalizeSector(filters.sector)}`);
    }
    if (filters?.exchange) {
      filterParts.push(`exch_${filters.exchange.toLowerCase()}`);
    }
    if (filters?.marketCap) {
      filterParts.push(this.getMarketCapFilter(filters.marketCap));
    }

    if (filterParts.length > 0) {
      params.f = filterParts.join(",");
    }

    const response = await this.axios.get("/screener.ashx", { params });
    const $ = cheerio.load(response.data);

    const tickers: string[] = [];

    // Extract tickers from links
    $('a[href*="quote.ashx?t="]').each((_, el) => {
      const href = $(el).attr("href");
      if (href) {
        const match = href.match(/t=([A-Z]+)/);
        if (match && match[1]) {
          tickers.push(match[1]);
        }
      }
    });

    return [...new Set(tickers)];
  }

  private normalizeExchange(exchange: string): string {
    const exchangeMap: Record<string, string> = {
      NASD: "NASDAQ",
      NYSE: "NYSE",
      AMEX: "AMEX",
    };
    return exchangeMap[exchange] || exchange;
  }

  private normalizeSector(sector: string): string {
    return sector.toLowerCase().replace(/\s+/g, "");
  }

  private getMarketCapFilter(marketCap: string): string {
    const filters: Record<string, string> = {
      mega: "cap_mega",
      large: "cap_large",
      mid: "cap_mid",
      small: "cap_small",
      micro: "cap_micro",
    };
    return filters[marketCap.toLowerCase()] || "cap_large";
  }

  clearCache(): void {
    this.cache.clear();
  }
}
