#!/usr/bin/env node
const { ESLint } = require('eslint');
const glob = require('glob');
const path = require('path');

(async function main() {
  const args = process.argv.slice(2);
  const fix = args.includes('--fix');

  const patterns = ['**/*.js', '**/*.ts'];
  const ignore = ['**/node_modules/**', '**/dist/**', '**/.git/**', '**/out/**', '**/.vscode/**'];

  let files = [];
  for (const p of patterns) {
    const found = glob.sync(p, { ignore, nodir: true });
    files = files.concat(found);
  }

  files = [...new Set(files)];

  if (files.length === 0) {
    console.log('No JS/TS files found to lint. Skipping ESLint.');
    process.exit(0);
  }

  try {
    const eslint = new ESLint({ fix, cwd: process.cwd() });
    const results = await eslint.lintFiles(files);
    if (fix) {
      await ESLint.outputFixes(results);
    }
    const formatter = await eslint.loadFormatter('stylish');
    const resultText = formatter.format(results);
    if (resultText && resultText.trim()) {
      console.log(resultText);
    } else {
      console.log('ESLint: No problems found');
    }

    const hasError = results.some((r) => r.errorCount > 0);
    process.exit(hasError ? 1 : 0);
  } catch (err) {
    console.error('ESLint wrapper error:', err);
    process.exit(2);
  }
})();
