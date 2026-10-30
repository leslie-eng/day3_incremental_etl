# Day 3 Incremental ETL Pipeline

A Python exercise that incrementally loads payment transaction batches into a CSV warehouse. It is designed to process each incoming JSON batch once, distinguish new, updated, duplicate, and invalid records, and persist checkpoint and log state.

## Features

- Loads the existing transaction warehouse from `data/transactions.csv`.
- Reads JSON transaction batches from `incoming/`.
- Provides classification and validation helpers for incremental upserts.
- Supports checkpointing and rotating log output.
- Uses an installable `day3-incremental-etl` Python package entry point.

> [!NOTE]
> Several functions in `incremental-etl.py` are currently exercises marked with `NotImplementedError` or `TODO`, including batch loading, validation, classification, checkpointing, and warehouse saving.

## Prerequisites

- Python 3.14 or newer
- `uv` for the project workflow configured in `pyproject.toml`
- Pandas 3.1.0 or newer

## Installation

From the project directory, create and install the project environment with `uv`:

```bash
uv sync
```

## Usage

Run the root script with Python:

```bash
python incremental-etl.py
```

After the project is implemented, the configured package command can be used with:

```bash
uv run day3-incremental-etl
```

The pipeline expects transaction batches in JSON format under `incoming/` and the existing warehouse at `data/transactions.csv`.

## Project Structure

```text
.
├── incremental-etl.py       # Current ETL implementation and pipeline entry flow
├── incoming/                # JSON transaction batches
├── data/transactions.csv    # Existing transaction warehouse
├── state/                   # Incremental processing checkpoint data
├── logs/                    # Runtime log output
├── src/day3_incremental_etl # Installable package entry point
├── pyproject.toml           # Project and dependency configuration
└── requirements.txt         # Dependency file placeholder
```

## Input Format

Each batch JSON file contains a `batch_id` and a `transactions` array. Each transaction includes fields such as:

- `transaction_id`
- `customer_id`
- `merchant`
- `amount_kes`
- `status`
- `channel`
- `updated_at`

The sample batch is available at `incoming/batch_2026-10-21.json`.

## Testing

No automated test suite is currently included in the repository.