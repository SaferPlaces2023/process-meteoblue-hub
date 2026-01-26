# Copilot GIS Orchestra v2 - AI Agent Instructions

**Purpose**: Workspace-wide instructions that define project context, architecture
patterns, and output constraints. Automatically applied to all Copilot chat
requests within this workspace.

## Project Context

This is a **knowledge repository** for Geographic Information Systems (GIS), urban planning, and AI-driven geospatial applications. The project is in early-stage development, structured around machine-readable configurations and context files rather than traditional application code.

## Architecture Pattern

The project follows a **context-driven architecture** where structured context,
not code, is the primary source of truth. Agents must treat context files as
stable contracts, not editable notes.

### Core Principles

- **Context is code**: every change is versioned, validated, and traceable
- **Orchestration ≠ logic**: workflows describe order, not implementation
- **Small, stable, reusable units** beat monolithic definitions
- **Append > modify** when evolution is uncertain

---

### Context Registry

**`context/index.json`** is the single orchestration authority.

It:

- declares workflows and their ordered steps
- defines inputs, outputs, dependencies, schedule, and version
- references step definitions by `id` (never inline)
- is read by agents as the only workflow discovery mechanism

Rules:

- no step logic inside `index.json`
- no silent edits (version bump required)
- removing entries requires explicit deprecation

---

### Structured Knowledge

**`context/*.json`** files are atomic, reusable knowledge units:

- step definitions
- templates
- schemas
- JSON-LD fragments
- static reference data

Each file MUST include:

```json
{
  "id": "string",
  "version": "x.y.z",
  "description": "string",
  "status": "draft|stable|deprecated|experimental"
}
```

## Source of Truth Hierarchy

When resolving conflicts or ambiguity, follow this priority order:

1. `.github/copilot-instructions.md` - Workspace-wide rules
2. `.github/instructions/*.instructions.md` - Conditional instructions
3. `context/index.json` - Workflow definitions
4. `context/*.json` - Structured knowledge files
5. `docs/` - Reference documentation for different resource types

## .github Directory Purpose

| Directory       | Purpose                                                                | Reference                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `agents/`       | Custom GitHub Copilot agent definitions (`.agent.md` files)            | [awesome-copilot/agents](https://github.com/github/awesome-copilot/tree/main/agents)                                                                      |
| `prompts/`      | Task-specific prompts (`.prompt.md` files)                             | [awesome-copilot/prompts](https://github.com/github/awesome-copilot/tree/main/prompts)                                                                    |
| `instructions/` | Coding standards and guidelines (`.instructions.md` files)             | [awesome-copilot/instructions](https://github.com/github/awesome-copilot/tree/main/instructions)                                                          |
| `skills/`       | Agent skill folders (each with `SKILL.md` and optional bundled assets) | [awesome-copilot/skills](https://github.com/github/awesome-copilot/tree/main/skills), [anthropics/skills](https://github.com/anthropics/skills/tree/main) |
| `eng/`          | Build and automation scripts                                           | —                                                                                                                                                         |
| `scripts/`      | Utility scripts                                                        | —                                                                                                                                                         |

## Output Constraints

### ALWAYS Produce

- Machine-readable formats: JSON, YAML, code, schemas
- Markdown documentation with strict formatting (see below) — only if requested during agent tasks
- Valid, parseable output even when information is incomplete

### NEVER Produce

- Conversational prose, greetings, or filler text
- Natural language error messages (use structured error objects)

### Markdown Conventions

Follow established community standards:

- [CommonMark](https://commonmark.org/) - Specification compliance
- [Google Markdown Style Guide](https://google.github.io/styleguide/docguide/style.html) - Style rules
- [Prettier](https://prettier.io/docs/en/options.html#prose-wrap) - Formatting defaults

## Annotation Markers

Use structured markers instead of prose comments. No docstrings or explanatory text.

| Marker               | Purpose          | Example                              |
| -------------------- | ---------------- | ------------------------------------ |
| `TODO(agent):`       | Action required  | `TODO(agent): implement validation`  |
| `ASSUMPTION(agent):` | Decision context | `ASSUMPTION(agent): using WGS84 CRS` |

Syntax by language:

- JavaScript/TypeScript/JSON: `// TODO(agent): ...`
- Python/YAML: `# TODO(agent): ...`
- HTML/XML: `<!-- TODO(agent): ... -->`

## Incomplete Operation Handling

When operations cannot complete, return valid, parseable output:

| Field    | Value              | Purpose                  |
| -------- | ------------------ | ------------------------ |
| `status` | `"pending"`        | Signals incomplete state |
| `reason` | String             | Machine-readable cause   |
| `action` | `TODO(agent): ...` | Required next step       |

```json
{
  "status": "pending",
  "reason": "missing_dependency:context/index.json",
  "action": "TODO(agent): Create workflow manifest"
}
```

**Constraints**: Output must remain parseable/compilable. Never return raw error messages.

## File Structure

| Directory      | Purpose                                              |
| -------------- | ---------------------------------------------------- |
| `context/`     | JSON-LD and structured data                          |
| `data/`        | Data pipeline stages (raw → interim → processed)     |
| `docs/`        | Static reference documentation                       |
| `projects/`    | Project conductors with configurations and pipelines |
| `data-schema/` | API/DATA schemas                                     |

### Projects Directory Layout

The `projects/` folder contains **conductors** — self-contained project units with specific configurations, metadata, and processing pipelines.

Projects may include supplementary files, like: QGIS projects, Issue tracking, Data manifests, ...

```
projects/
└── {unique-slug-of-project}/
    ├── configuration.json   # Project settings and parameters
    ├── metadata.json        # Descriptive information and provenance
    └── pipeline.json        # Data processing workflow definitions
```

| File                 | Purpose                                                           |
| -------------------- | ----------------------------------------------------------------- |
| `configuration.json` | Runtime settings, feature flags, connection strings               |
| `metadata.json`      | Project identity, ownership, dates, tags, linked resources        |
| `pipeline.json`      | Ordered processing steps, input/output mappings, task definitions |

#### Naming Convention

- Use lowercase kebab-case for project slugs: `downtown-transit-study`
- Slugs must be unique across the workspace
- Avoid special characters except hyphens

### Data Directory Layout

| Subdirectory      | Contents                       |
| ----------------- | ------------------------------ |
| `data/raw/`       | Immutable source data          |
| `data/external/`  | Third-party reference datasets |
| `data/interim/`   | Intermediate transformations   |
| `data/processed/` | Final analysis-ready outputs   |

## Integration Points

- **Context System**: All structured knowledge anchors to `context/index.json`
