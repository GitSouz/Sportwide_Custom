# Sportwide_Custom

## Database

The Azure SQL database schema is versioned in [`db/`](db/) as ordered SQL
migrations. See [`db/README.md`](db/README.md) for how to apply and extend it.

## Data sync

Daily Databricks → Azure SQL sync of ticket-transaction data lives in
[`sync/`](sync/). See [`sync/README.md`](sync/README.md) for how it's scheduled
and configured.
