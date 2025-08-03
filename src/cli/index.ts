#!/usr/bin/env bun

import { Cli, Builtins } from 'clipanion';
import { FetchCommand } from './commands/fetch';

const cli = new Cli({
  binaryLabel: 'stocker',
  binaryName: 'stocker',
  binaryVersion: '1.0.0',
});

cli.register(FetchCommand);
cli.register(Builtins.HelpCommand);
cli.register(Builtins.VersionCommand);

cli.runExit(process.argv.slice(2));