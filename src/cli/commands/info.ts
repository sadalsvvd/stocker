import { Command, Option } from "clipanion";
import { Stocker } from "../../index";
import * as t from "typanion";

export class InfoCommand extends Command {
  static override paths = [["info"]];

  static override usage = Command.Usage({
    description: "Show metadata about a ticker",
    details: `
      This command displays detailed information about a ticker including:
      - First and last date of data
      - Total number of records
      - Data source
      - Last update time
      - Any significant data gaps (>10 trading days)
      
      Use --all-gaps to show all gaps including holidays and minor gaps.
    `,
    examples: [
      ["Show info for AAPL", "stocker info AAPL"],
      ["Show all gaps including holidays", "stocker info AAPL --all-gaps"],
    ],
  });

  ticker = Option.String({ required: true });
  
  allGaps = Option.Boolean("--all-gaps", false, {
    description: "Show all gaps including holidays and minor gaps",
  });

  async execute() {
    const stocker = new Stocker();
    await stocker.init();

    try {
      await stocker.info(this.ticker, { showAllGaps: this.allGaps });
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      console.error(`Failed to get info: ${errorMessage}`);
    } finally {
      await stocker.close();
    }
  }
}
