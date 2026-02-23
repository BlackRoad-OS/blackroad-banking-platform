# BlackRoad Banking Platform

> Banking core: accounts, transfers, statements, interest — ACID transactions with SQLite and decimal precision.

Part of the [BlackRoad OS](https://github.com/BlackRoad-OS) platform.

## Features

- **Account management**: `open_account()`, `freeze_account()`, `close_account()`
- **ACID transfers**: Atomic double-entry with deadlock-safe ordering
- **Decimal precision**: `Decimal` throughout — zero floating-point errors
- **Statements**: 30-day transaction history with CSV export
- **Interest**: Apply compounding interest to savings/money-market accounts
- **Account types**: Checking, Savings, Money Market, Credit

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Open account
python src/banking_platform.py open "Alice" 1000.00 --type savings --rate 0.045

# Check balance
python src/banking_platform.py balance <account-id>

# Transfer
python src/banking_platform.py transfer <from-id> <to-id> 250.00

# Statement
python src/banking_platform.py statement <account-id> --days 30

# Apply interest
python src/banking_platform.py interest <account-id>

# Freeze account
python src/banking_platform.py freeze <account-id>
```

## Testing

```bash
pytest tests/ -v --tb=short
```

## Architecture

- `src/banking_platform.py` — 760+ lines: `Account`, `Transaction`, `BankingDB`, `BankingService`
- `tests/test_banking.py` — 18 test functions covering ACID, precision, edge cases
- SQLite with WAL mode, foreign keys, FULL synchronous for durability

## License

Proprietary — © BlackRoad OS, Inc. All rights reserved.
