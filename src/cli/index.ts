#!/usr/bin/env bun

import { Cli, Builtins } from 'clipanion';
import { FetchCommand } from './commands/fetch';
import { ListCommand } from './commands/list';
import { InfoCommand } from './commands/info';
import { 
  TickersCommand, 
  TickersUpdateCommand, 
  TickersSearchCommand, 
  TickersStatsCommand 
} from './commands/tickers';

const cli = new Cli({
  binaryLabel: 'stocker',
  binaryName: 'stocker',
  binaryVersion: '1.0.0',
});

cli.register(FetchCommand);
cli.register(ListCommand);
cli.register(InfoCommand);
cli.register(TickersCommand);
cli.register(TickersUpdateCommand);
cli.register(TickersSearchCommand);
cli.register(TickersStatsCommand);
cli.register(Builtins.HelpCommand);
cli.register(Builtins.VersionCommand);

cli.runExit(process.argv.slice(2));