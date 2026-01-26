#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const glob = require('glob');
const Ajv = require('ajv').default;
const addFormats = require('ajv-formats');

const root = process.cwd();
const schemaDir = path.join(root, 'data-schema');

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);

const mapping = {
  'projects/**/configuration.json': 'project-configuration.schema.json',
  'projects/**/metadata.json': 'project-metadata.schema.json',
  'projects/**/pipeline.json': 'project-pipeline.schema.json',
};

let failed = false;

for (const [pattern, schemaFile] of Object.entries(mapping)) {
  const schemaPath = path.join(schemaDir, schemaFile);
  if (!fs.existsSync(schemaPath)) {
    console.warn(
      `warning: missing schema ${schemaFile} (expected at ${schemaPath}), skipping pattern ${pattern}`,
    );
    continue;
  }

  const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
  const validate = ajv.compile(schema);
  const files = glob.sync(pattern, { cwd: root, absolute: true });
  if (files.length === 0) {
    // no files to validate for this pattern
    continue;
  }

  for (const file of files) {
    try {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      const ok = validate(data);
      if (!ok) {
        console.error(`\n✖ ${path.relative(root, file)} ⟵ failed ${schemaFile}`);
        console.error(validate.errors);
        failed = true;
      } else {
        console.log(`✔ ${path.relative(root, file)}`);
      }
    } catch (err) {
      console.error(`\n✖ ${path.relative(root, file)} ⟵ parse-error: ${err.message}`);
      failed = true;
    }
  }
}

process.exit(failed ? 1 : 0);
