import { Stocker } from "./src/index.ts";

async function test() {
  const stocker = new Stocker({
    sources: {
      eodhd: {
        apiKey: process.env.EODHD_API_KEY || "",
      },
    },
  });

  try {
    await stocker.init();
    console.log("Stocker initialized");

    // Test fetching a small amount of data
    await stocker.fetch("AAPL", {
      start: "2024-01-01",
      end: "2024-01-10",
    });

    // List tickers
    const tickers = await stocker.list();
    console.log("Stored tickers:", tickers);

    // Get info
    await stocker.info("AAPL");

    // Test update
    console.log("\nTesting update...");
    await stocker.fetch("AAPL", {
      update: true,
    });
  } catch (error) {
    console.error("Error:", error);
  }
}

if (import.meta.main) {
  test();
}
