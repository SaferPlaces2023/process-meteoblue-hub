#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const glob = require('glob');

const root = process.cwd();
const tsconfig = path.join(root, 'tsconfig.json');

const tsFiles = glob.sync('**/*.ts', {
  ignore: ['**/node_modules/**', '**/dist/**', '**/out/**', '**/.git/**'],
});

if (tsFiles.length === 0 && !fs.existsSync(tsconfig)) {
  console.log('No TypeScript files or tsconfig.json found. Skipping TypeScript validation.');
  process.exit(0);
}

let cmd = 'tsc';
let args = ['--noEmit'];

if (fs.existsSync(tsconfig)) {
  // run using the project configuration
  console.log('Found tsconfig.json — running `tsc --noEmit`');
} else {
  // run against discovered files
  console.log(`Found ${tsFiles.length} TypeScript files — running tsc on files`);
  args = args.concat(tsFiles.slice(0, 200)); // limit to 200 files to avoid command length issues
}

const res = spawnSync(cmd, args, { stdio: 'inherit', shell: true });
process.exit(res.status);
