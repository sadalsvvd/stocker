import { Command } from "clipanion";
import { Stocker } from "../../index";

export class ListCommand extends Command {
  static override paths = [["list"]];

  static override usage = Command.Usage({
    description: "List all tickers in the local store",
    details: `
      This command lists all tickers that have been fetched and stored locally.
      It shows the last update date and record count for each ticker.
    `,
    examples: [["List all stored tickers", "stocker list"]],
  });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    try {
      const tickers = await stocker.list();

      if (tickers.length === 0) {
        console.log("No tickers found in local store");
        return;
      }

      console.log(`Found ${tickers.length} ticker(s):\n`);

      // Get metadata for each ticker
      for (const ticker of tickers.sort()) {
        try {
          const metadata = await stocker.storage.getMetadata(ticker);
          if (metadata && metadata.recordCount !== null) {
            const lastUpdate = new Date(
              metadata.lastUpdate
            ).toLocaleDateString();
            const firstTradeDate = metadata.firstTradeDate
              ? new Date(metadata.firstTradeDate).toISOString().split("T")[0]
              : "N/A";
            console.log(
              `${ticker.padEnd(10)} ${metadata.recordCount
                .toString()
                .padStart(6)} records, ` + `${firstTradeDate} to ${lastUpdate}`
            );
          } else {
            // Try to get data directly if metadata is missing or incomplete
            const data = await stocker.storage.getDaily(ticker);
            if (data.length > 0) {
              const firstDate = data[0]?.date;
              const lastDate = data[data.length - 1]?.date;
              const formatDate = (d: string | Date | undefined) => {
                if (!d) return "N/A";
                if (typeof d === "string") return d;
                return new Date(d).toISOString().split("T")[0];
              };
              console.log(
                `${ticker.padEnd(10)} ${data.length
                  .toString()
                  .padStart(6)} records, ` +
                  `${formatDate(firstDate)} to ${formatDate(lastDate)}`
              );
            } else {
              console.log(`${ticker.padEnd(10)} No data available`);
            }
          }
        } catch (error) {
          console.log(`${ticker.padEnd(10)} Error reading data`);
        }
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.error(`Failed to list tickers: ${errorMessage}`);
    }
  }
}
