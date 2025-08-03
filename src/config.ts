import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import yaml from "yaml";
import dotenv from "dotenv";
import type { Config } from "./types/index.ts";

// Load environment variables
dotenv.config();

const DEFAULT_CONFIG_PATH = join(homedir(), ".stocker", "config.yml");
const DEFAULT_DATA_DIR = join(process.cwd(), "data");

// Time intervals
export const TIMEFRAME_1_MINUTE = "1min";
export const TIMEFRAME_5_MINUTES = "5min";
export const TIMEFRAME_15_MINUTES = "15min";
export const TIMEFRAME_30_MINUTES = "30min";
export const TIMEFRAME_1_HOUR = "1h";
export const TIMEFRAME_1_DAY = "1d";
export const TIMEFRAME_1_WEEK = "1w";
export const TIMEFRAME_1_MONTH = "1m";

export function loadConfig(
  configPath?: string,
  overrides?: Partial<Config>
): Config {
  const path = configPath || DEFAULT_CONFIG_PATH;

  let fileConfig: Partial<Config> = {};

  if (existsSync(path)) {
    try {
      const fileContent = readFileSync(path, "utf-8");
      fileConfig = yaml.parse(fileContent) || {};
    } catch (error) {
      console.warn(`Failed to load config from ${path}:`, error);
    }
  }

  // Merge file config with environment variables and overrides
  const config: Config = {
    sources: {
      eodhd: {
        apiKey:
          overrides?.sources?.eodhd?.apiKey ||
          process.env.EODHD_API_KEY ||
          fileConfig.sources?.eodhd?.apiKey ||
          "",
        rateLimit:
          overrides?.sources?.eodhd?.rateLimit ||
          fileConfig.sources?.eodhd?.rateLimit ||
          20,
      },
    },
    storage: {
      dataDir:
        overrides?.storage?.dataDir ||
        fileConfig.storage?.dataDir ||
        DEFAULT_DATA_DIR,
    },
    defaults: {
      startDate:
        overrides?.defaults?.startDate ||
        fileConfig.defaults?.startDate ||
        "2020-01-01",
      parallel:
        overrides?.defaults?.parallel || fileConfig.defaults?.parallel || 5,
    },
  };

  return config;
}

// Create a mutable config instance
let configInstance: Config = loadConfig();

// Export functions to work with config
export const getConfig = () => configInstance;
export const setConfig = (config: Config) => {
  configInstance = config;
};
export const updateConfig = (overrides: Partial<Config>) => {
  configInstance = loadConfig(undefined, overrides);
};

// Export for backwards compatibility
export const CONFIG = configInstance;
