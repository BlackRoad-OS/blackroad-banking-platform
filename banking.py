#!/usr/bin/env python3
"""
BlackRoad Banking Platform
Production-grade banking system with accounts, transactions, interest, and anomaly detection.
"""
from __future__ import annotations
import argparse
import json
import math
import os
import sqlite3
import statistics
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

DB_PATH = os.path.expanduser("~/.blackroad/banking.db")


@dataclass
class Account:
    id: str
    owner: str
    type: str          # checking | savings | investment
    balance: float
    currency: str
    iban: str
    routing: str
    created_at: str
    frozen: bool = False
    interest_rate: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Transaction:
    id: str
    from_account: str
    to_account: str
    amount: float
    currency: str
    type: str          # debit | credit | transfer | fee | interest
    status: str        # pending | completed | failed | reversed
    memo: str
    created_at: str
    fee: float = 0.0
    reference: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditLog:
    id: str
    account_id: str
    action: str
    actor: str
    details: str
    created_at: str

    def to_dict(self) -> dict:
        return asdict(self)


def _now() -> str:
    return datetime.utcnow().isoformat()


def _gen_iban(country: str = "US") -> str:
    import random
    return f"{country}{random.randint(10,99)}{''.join([str(random.randint(0,9)) for _ in range(18)])}"


def _gen_routing() -> str:
    import random
    return "".join([str(random.randint(0, 9)) for _ in range(9)])


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

def get_db(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path: str = DB_PATH) -> None:
    with get_db(path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS accounts (
                id           TEXT PRIMARY KEY,
                owner        TEXT NOT NULL,
                type         TEXT NOT NULL CHECK(type IN ('checking','savings','investment')),
                balance      REAL NOT NULL DEFAULT 0,
                currency     TEXT NOT NULL DEFAULT 'USD',
                iban         TEXT UNIQUE NOT NULL,
                routing      TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                frozen       INTEGER NOT NULL DEFAULT 0,
                interest_rate REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id           TEXT PRIMARY KEY,
                from_account TEXT,
                to_account   TEXT,
                amount       REAL NOT NULL,
                currency     TEXT NOT NULL DEFAULT 'USD',
                type         TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                memo         TEXT NOT NULL DEFAULT '',
                created_at   TEXT NOT NULL,
                fee          REAL NOT NULL DEFAULT 0,
                reference    TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id           TEXT PRIMARY KEY,
                account_id   TEXT NOT NULL,
                action       TEXT NOT NULL,
                actor        TEXT NOT NULL,
                details      TEXT NOT NULL DEFAULT '',
                created_at   TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_txn_from    ON transactions(from_account);
            CREATE INDEX IF NOT EXISTS idx_txn_to      ON transactions(to_account);
            CREATE INDEX IF NOT EXISTS idx_txn_created ON transactions(created_at);
            CREATE INDEX IF NOT EXISTS idx_audit_acc   ON audit_log(account_id);
        """)


# ---------------------------------------------------------------------------
# Core banking operations
# ---------------------------------------------------------------------------

def open_account(
    owner: str,
    account_type: str,
    initial_deposit: float = 0.0,
    currency: str = "USD",
    interest_rate: float = 0.0,
    path: str = DB_PATH,
) -> Account:
    """Open a new bank account with an optional initial deposit."""
    if account_type not in ("checking", "savings", "investment"):
        raise ValueError(f"Invalid account type: {account_type}")
    if initial_deposit < 0:
        raise ValueError("Initial deposit cannot be negative")

    acc = Account(
        id=str(uuid.uuid4()),
        owner=owner,
        type=account_type,
        balance=initial_deposit,
        currency=currency,
        iban=_gen_iban(),
        routing=_gen_routing(),
        created_at=_now(),
        interest_rate=interest_rate,
    )
    with get_db(path) as conn:
        conn.execute(
            """INSERT INTO accounts
               (id, owner, type, balance, currency, iban, routing, created_at, frozen, interest_rate)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (acc.id, acc.owner, acc.type, acc.balance, acc.currency,
             acc.iban, acc.routing, acc.created_at, int(acc.frozen), acc.interest_rate),
        )
        if initial_deposit > 0:
            txn = Transaction(
                id=str(uuid.uuid4()),
                from_account="SYSTEM",
                to_account=acc.id,
                amount=initial_deposit,
                currency=currency,
                type="credit",
                status="completed",
                memo="Initial deposit",
                created_at=_now(),
            )
            conn.execute(
                """INSERT INTO transactions
                   (id, from_account, to_account, amount, currency, type, status, memo, created_at, fee, reference)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (txn.id, txn.from_account, txn.to_account, txn.amount, txn.currency,
                 txn.type, txn.status, txn.memo, txn.created_at, txn.fee, txn.reference),
            )
        _audit(conn, acc.id, "open_account", owner,
               f"type={account_type}, initial_deposit={initial_deposit}")
    return acc


def get_account(account_id: str, path: str = DB_PATH) -> Account:
    with get_db(path) as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    if not row:
        raise KeyError(f"Account {account_id} not found")
    return _row_to_account(row)


def get_balance(account_id: str, path: str = DB_PATH) -> float:
    return get_account(account_id, path).balance


def transfer(
    from_id: str,
    to_id: str,
    amount: float,
    memo: str = "",
    fee_rate: float = 0.001,
    path: str = DB_PATH,
) -> Tuple[Transaction, Transaction]:
    """Transfer funds between accounts, deducting a fee from the sender."""
    if amount <= 0:
        raise ValueError("Transfer amount must be positive")
    fee = round(amount * fee_rate, 2)
    total_debit = amount + fee

    with get_db(path) as conn:
        src_row = conn.execute(
            "SELECT * FROM accounts WHERE id=?", (from_id,)
        ).fetchone()
        dst_row = conn.execute(
            "SELECT * FROM accounts WHERE id=?", (to_id,)
        ).fetchone()

        if not src_row:
            raise KeyError(f"Source account {from_id} not found")
        if not dst_row:
            raise KeyError(f"Destination account {to_id} not found")

        src = _row_to_account(src_row)
        dst = _row_to_account(dst_row)

        if src.frozen:
            raise PermissionError(f"Account {from_id} is frozen")
        if dst.frozen:
            raise PermissionError(f"Account {to_id} is frozen")
        if src.balance < total_debit:
            raise ValueError(
                f"Insufficient funds: balance={src.balance}, required={total_debit}"
            )

        ref = str(uuid.uuid4())[:8].upper()
        now = _now()

        debit_txn = Transaction(
            id=str(uuid.uuid4()),
            from_account=from_id,
            to_account=to_id,
            amount=amount,
            currency=src.currency,
            type="transfer",
            status="completed",
            memo=memo,
            created_at=now,
            fee=fee,
            reference=ref,
        )
        credit_txn = Transaction(
            id=str(uuid.uuid4()),
            from_account=from_id,
            to_account=to_id,
            amount=amount,
            currency=dst.currency,
            type="transfer",
            status="completed",
            memo=memo,
            created_at=now,
            fee=0.0,
            reference=ref,
        )

        conn.execute(
            "UPDATE accounts SET balance=balance-? WHERE id=?", (total_debit, from_id)
        )
        conn.execute(
            "UPDATE accounts SET balance=balance+? WHERE id=?", (amount, to_id)
        )
        for t in (debit_txn, credit_txn):
            conn.execute(
                """INSERT INTO transactions
                   (id, from_account, to_account, amount, currency, type, status, memo, created_at, fee, reference)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (t.id, t.from_account, t.to_account, t.amount, t.currency,
                 t.type, t.status, t.memo, t.created_at, t.fee, t.reference),
            )
        _audit(conn, from_id, "transfer_out", "system",
               f"to={to_id}, amount={amount}, fee={fee}, ref={ref}")
        _audit(conn, to_id, "transfer_in", "system",
               f"from={from_id}, amount={amount}, ref={ref}")

    return debit_txn, credit_txn


def get_statement(
    account_id: str,
    start: str,
    end: str,
    path: str = DB_PATH,
) -> List[Transaction]:
    """Return all transactions for an account within a date range."""
    with get_db(path) as conn:
        rows = conn.execute(
            """SELECT * FROM transactions
               WHERE (from_account=? OR to_account=?)
                 AND created_at BETWEEN ? AND ?
               ORDER BY created_at DESC""",
            (account_id, account_id, start, end),
        ).fetchall()
    return [_row_to_txn(r) for r in rows]


def calculate_interest(
    account_id: str,
    rate: Optional[float] = None,
    path: str = DB_PATH,
) -> float:
    """Apply daily interest and return the amount credited."""
    with get_db(path) as conn:
        row = conn.execute(
            "SELECT * FROM accounts WHERE id=?", (account_id,)
        ).fetchone()
        if not row:
            raise KeyError(f"Account {account_id} not found")
        acc = _row_to_account(row)
        effective_rate = rate if rate is not None else acc.interest_rate
        if effective_rate <= 0:
            return 0.0
        daily_rate = effective_rate / 365
        interest = round(acc.balance * daily_rate, 4)
        if interest > 0:
            now = _now()
            conn.execute(
                "UPDATE accounts SET balance=balance+? WHERE id=?", (interest, account_id)
            )
            txn_id = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO transactions
                   (id, from_account, to_account, amount, currency, type, status, memo, created_at, fee, reference)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (txn_id, "SYSTEM", account_id, interest, acc.currency,
                 "interest", "completed", "Daily interest credit", now, 0.0, ""),
            )
            _audit(conn, account_id, "interest_credit", "system", f"rate={effective_rate}, amount={interest}")
    return interest


def freeze_account(account_id: str, actor: str = "system", path: str = DB_PATH) -> None:
    """Freeze an account to prevent transactions."""
    with get_db(path) as conn:
        conn.execute("UPDATE accounts SET frozen=1 WHERE id=?", (account_id,))
        _audit(conn, account_id, "freeze", actor, "Account frozen")


def unfreeze_account(account_id: str, actor: str = "system", path: str = DB_PATH) -> None:
    """Unfreeze an account."""
    with get_db(path) as conn:
        conn.execute("UPDATE accounts SET frozen=0 WHERE id=?", (account_id,))
        _audit(conn, account_id, "unfreeze", actor, "Account unfrozen")


def detect_anomaly(
    account_id: str,
    std_dev_threshold: float = 3.0,
    path: str = DB_PATH,
) -> List[Transaction]:
    """Flag transactions that deviate more than `std_dev_threshold` standard deviations from the mean."""
    with get_db(path) as conn:
        rows = conn.execute(
            """SELECT * FROM transactions
               WHERE (from_account=? OR to_account=?) AND status='completed'
               ORDER BY created_at""",
            (account_id, account_id),
        ).fetchall()
    txns = [_row_to_txn(r) for r in rows]
    if len(txns) < 3:
        return []
    amounts = [t.amount for t in txns]
    mean = statistics.mean(amounts)
    stdev = statistics.stdev(amounts)
    if stdev == 0:
        return []
    return [t for t in txns if abs(t.amount - mean) > std_dev_threshold * stdev]


def list_accounts(path: str = DB_PATH) -> List[Account]:
    with get_db(path) as conn:
        rows = conn.execute("SELECT * FROM accounts ORDER BY created_at DESC").fetchall()
    return [_row_to_account(r) for r in rows]


def list_transactions(
    account_id: Optional[str] = None,
    limit: int = 50,
    path: str = DB_PATH,
) -> List[Transaction]:
    with get_db(path) as conn:
        if account_id:
            rows = conn.execute(
                """SELECT * FROM transactions
                   WHERE from_account=? OR to_account=?
                   ORDER BY created_at DESC LIMIT ?""",
                (account_id, account_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
    return [_row_to_txn(r) for r in rows]


def deposit(
    account_id: str,
    amount: float,
    memo: str = "Deposit",
    path: str = DB_PATH,
) -> Transaction:
    """Deposit funds into an account."""
    if amount <= 0:
        raise ValueError("Deposit amount must be positive")
    with get_db(path) as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if not row:
            raise KeyError(f"Account {account_id} not found")
        acc = _row_to_account(row)
        if acc.frozen:
            raise PermissionError(f"Account {account_id} is frozen")
        conn.execute("UPDATE accounts SET balance=balance+? WHERE id=?", (amount, account_id))
        txn = Transaction(
            id=str(uuid.uuid4()),
            from_account="EXTERNAL",
            to_account=account_id,
            amount=amount,
            currency=acc.currency,
            type="credit",
            status="completed",
            memo=memo,
            created_at=_now(),
        )
        conn.execute(
            """INSERT INTO transactions
               (id, from_account, to_account, amount, currency, type, status, memo, created_at, fee, reference)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (txn.id, txn.from_account, txn.to_account, txn.amount, txn.currency,
             txn.type, txn.status, txn.memo, txn.created_at, txn.fee, txn.reference),
        )
        _audit(conn, account_id, "deposit", "system", f"amount={amount}")
    return txn


def withdraw(
    account_id: str,
    amount: float,
    memo: str = "Withdrawal",
    path: str = DB_PATH,
) -> Transaction:
    """Withdraw funds from an account."""
    if amount <= 0:
        raise ValueError("Withdrawal amount must be positive")
    with get_db(path) as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if not row:
            raise KeyError(f"Account {account_id} not found")
        acc = _row_to_account(row)
        if acc.frozen:
            raise PermissionError(f"Account {account_id} is frozen")
        if acc.balance < amount:
            raise ValueError(f"Insufficient funds: balance={acc.balance}, required={amount}")
        conn.execute("UPDATE accounts SET balance=balance-? WHERE id=?", (amount, account_id))
        txn = Transaction(
            id=str(uuid.uuid4()),
            from_account=account_id,
            to_account="EXTERNAL",
            amount=amount,
            currency=acc.currency,
            type="debit",
            status="completed",
            memo=memo,
            created_at=_now(),
        )
        conn.execute(
            """INSERT INTO transactions
               (id, from_account, to_account, amount, currency, type, status, memo, created_at, fee, reference)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (txn.id, txn.from_account, txn.to_account, txn.amount, txn.currency,
             txn.type, txn.status, txn.memo, txn.created_at, txn.fee, txn.reference),
        )
        _audit(conn, account_id, "withdraw", "system", f"amount={amount}")
    return txn


def get_audit_log(account_id: str, path: str = DB_PATH) -> List[AuditLog]:
    with get_db(path) as conn:
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE account_id=? ORDER BY created_at DESC",
            (account_id,),
        ).fetchall()
    return [AuditLog(
        id=r["id"], account_id=r["account_id"], action=r["action"],
        actor=r["actor"], details=r["details"], created_at=r["created_at"],
    ) for r in rows]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _audit(conn: sqlite3.Connection, account_id: str, action: str, actor: str, details: str) -> None:
    conn.execute(
        "INSERT INTO audit_log (id, account_id, action, actor, details, created_at) VALUES (?,?,?,?,?,?)",
        (str(uuid.uuid4()), account_id, action, actor, details, _now()),
    )


def _row_to_account(row: sqlite3.Row) -> Account:
    return Account(
        id=row["id"], owner=row["owner"], type=row["type"],
        balance=row["balance"], currency=row["currency"],
        iban=row["iban"], routing=row["routing"],
        created_at=row["created_at"], frozen=bool(row["frozen"]),
        interest_rate=row["interest_rate"],
    )


def _row_to_txn(row: sqlite3.Row) -> Transaction:
    return Transaction(
        id=row["id"], from_account=row["from_account"], to_account=row["to_account"],
        amount=row["amount"], currency=row["currency"], type=row["type"],
        status=row["status"], memo=row["memo"], created_at=row["created_at"],
        fee=row["fee"], reference=row["reference"],
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_json(obj) -> None:
    if hasattr(obj, "to_dict"):
        print(json.dumps(obj.to_dict(), indent=2))
    elif isinstance(obj, list):
        print(json.dumps([o.to_dict() if hasattr(o, "to_dict") else o for o in obj], indent=2))
    else:
        print(json.dumps(obj, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="banking",
        description="BlackRoad Banking Platform CLI",
    )
    parser.add_argument("--db", default=DB_PATH, help="SQLite database path")
    sub = parser.add_subparsers(dest="command")

    # init
    sub.add_parser("init", help="Initialize the database")

    # open-account
    p = sub.add_parser("open-account", help="Open a new account")
    p.add_argument("owner")
    p.add_argument("type", choices=["checking", "savings", "investment"])
    p.add_argument("--deposit", type=float, default=0.0)
    p.add_argument("--currency", default="USD")
    p.add_argument("--rate", type=float, default=0.0)

    # list-accounts
    sub.add_parser("list-accounts", help="List all accounts")

    # balance
    p = sub.add_parser("balance", help="Get account balance")
    p.add_argument("account_id")

    # deposit
    p = sub.add_parser("deposit", help="Deposit funds")
    p.add_argument("account_id")
    p.add_argument("amount", type=float)
    p.add_argument("--memo", default="Deposit")

    # withdraw
    p = sub.add_parser("withdraw", help="Withdraw funds")
    p.add_argument("account_id")
    p.add_argument("amount", type=float)
    p.add_argument("--memo", default="Withdrawal")

    # transfer
    p = sub.add_parser("transfer", help="Transfer between accounts")
    p.add_argument("from_id")
    p.add_argument("to_id")
    p.add_argument("amount", type=float)
    p.add_argument("--memo", default="")
    p.add_argument("--fee-rate", type=float, default=0.001)

    # statement
    p = sub.add_parser("statement", help="Get account statement")
    p.add_argument("account_id")
    p.add_argument("--start", default="2000-01-01")
    p.add_argument("--end", default="2099-12-31")

    # interest
    p = sub.add_parser("interest", help="Calculate and apply daily interest")
    p.add_argument("account_id")
    p.add_argument("--rate", type=float, default=None)

    # freeze
    p = sub.add_parser("freeze", help="Freeze an account")
    p.add_argument("account_id")
    p.add_argument("--actor", default="cli")

    # unfreeze
    p = sub.add_parser("unfreeze", help="Unfreeze an account")
    p.add_argument("account_id")
    p.add_argument("--actor", default="cli")

    # anomaly
    p = sub.add_parser("anomaly", help="Detect anomalous transactions")
    p.add_argument("account_id")
    p.add_argument("--threshold", type=float, default=3.0)

    # audit
    p = sub.add_parser("audit", help="Show audit log")
    p.add_argument("account_id")

    # transactions
    p = sub.add_parser("transactions", help="List transactions")
    p.add_argument("--account", default=None)
    p.add_argument("--limit", type=int, default=20)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    db = args.db if hasattr(args, "db") else DB_PATH
    init_db(db)

    if args.command == "init":
        print("Database initialized.")
    elif args.command == "open-account":
        acc = open_account(args.owner, args.type, args.deposit, args.currency, args.rate, db)
        _print_json(acc)
    elif args.command == "list-accounts":
        _print_json(list_accounts(db))
    elif args.command == "balance":
        bal = get_balance(args.account_id, db)
        print(json.dumps({"account_id": args.account_id, "balance": bal}))
    elif args.command == "deposit":
        txn = deposit(args.account_id, args.amount, args.memo, db)
        _print_json(txn)
    elif args.command == "withdraw":
        txn = withdraw(args.account_id, args.amount, args.memo, db)
        _print_json(txn)
    elif args.command == "transfer":
        d, c = transfer(args.from_id, args.to_id, args.amount, args.memo, args.fee_rate, db)
        _print_json([d, c])
    elif args.command == "statement":
        txns = get_statement(args.account_id, args.start, args.end, db)
        _print_json(txns)
    elif args.command == "interest":
        amount = calculate_interest(args.account_id, args.rate, db)
        print(json.dumps({"credited": amount}))
    elif args.command == "freeze":
        freeze_account(args.account_id, args.actor, db)
        print(json.dumps({"status": "frozen"}))
    elif args.command == "unfreeze":
        unfreeze_account(args.account_id, args.actor, db)
        print(json.dumps({"status": "unfrozen"}))
    elif args.command == "anomaly":
        flagged = detect_anomaly(args.account_id, args.threshold, db)
        _print_json(flagged)
    elif args.command == "audit":
        logs = get_audit_log(args.account_id, db)
        _print_json(logs)
    elif args.command == "transactions":
        txns = list_transactions(args.account, args.limit, db)
        _print_json(txns)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
