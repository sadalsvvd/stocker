import { Stocker } from "./src/index.ts";

async function testAdjustedPrices() {
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

    // Fetch AAPL data around a known split (Aug 31, 2020 was 4:1 split)
    await stocker.fetch("AAPL", {
      start: "2020-08-25",
      end: "2020-09-05",
    });

    // Read the data back
    const data = await stocker.storage.getDaily("AAPL");

    console.log("\nAAPL data around 4:1 split (Aug 31, 2020):");
    console.log("Date        | Raw Close | Adj Close | Raw Vol    | Adj Vol");
    console.log(
      "------------|-----------|-----------|------------|------------"
    );

    data.forEach((day) => {
      console.log(
        `${day.date} | ${day.close.toFixed(2).padStart(9)} | ${day.adjClose
          .toFixed(2)
          .padStart(9)} | ${day.volume
          .toLocaleString()
          .padStart(10)} | ${day.adjVolume.toLocaleString().padStart(10)}`
      );
    });

    // Show adjustment factor
    const beforeSplit = data.find((d) => d.date === "2020-08-28");
    const afterSplit = data.find((d) => d.date === "2020-08-31");

    if (beforeSplit && afterSplit) {
      const adjFactor = beforeSplit.adjClose / beforeSplit.close;
      console.log(
        `\nAdjustment factor: ${adjFactor.toFixed(4)} (indicates ${
          1 / adjFactor
        }:1 split)`
      );
    }
  } catch (error) {
    console.error("Error:", error);
  }
}

if (import.meta.main) {
  testAdjustedPrices();
}
