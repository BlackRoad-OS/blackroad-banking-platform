# blackroad-banking-platform

Production-grade banking platform with accounts, transfers, interest, fraud detection, and audit trails.

## Features
- Checking, savings, and investment accounts with IBAN/routing generation
- Transfers with configurable fee rates
- Daily interest calculation and crediting
- Account freeze/unfreeze controls
- Anomaly detection via statistical z-score analysis (flags transactions > 3σ from mean)
- Full audit log for all account actions
- SQLite persistence with WAL mode

## Usage
```bash
python banking.py init
python banking.py open-account Alice checking --deposit 1000
python banking.py transfer <from_id> <to_id> 200 --memo "Rent"
python banking.py statement <account_id> --start 2025-01-01
python banking.py anomaly <account_id>
python banking.py freeze <account_id>
python banking.py interest <account_id>
```

## Testing
```bash
pip install pytest
pytest test_banking.py -v
```
