"""
BlackRoad Banking Platform
==========================
Production-quality banking core with ACID transactions, decimal precision,
account management, and full statement history via SQLite.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from pathlib import Path
from typing import Generator, List, Optional

# ─── Configuration ────────────────────────────────────────────────────────────
DB_PATH = Path.home() / ".blackroad" / "banking.db"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
logger = logging.getLogger("banking")

PRECISION = Decimal("0.01")


# ─── Enumerations ─────────────────────────────────────────────────────────────
class AccountType(str, Enum):
    CHECKING = "checking"
    SAVINGS = "savings"
    MONEY_MARKET = "money_market"
    CREDIT = "credit"


class AccountStatus(str, Enum):
    ACTIVE = "active"
    FROZEN = "frozen"
    CLOSED = "closed"


class TransactionType(str, Enum):
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"
    INTEREST = "interest"
    FEE = "fee"


# ─── Data Classes ─────────────────────────────────────────────────────────────
@dataclass
class Account:
    id: str
    owner: str
    balance: Decimal
    currency: str
    account_type: AccountType
    status: AccountStatus = AccountStatus.ACTIVE
    created_at: datetime = field(default_factory=datetime.utcnow)
    interest_rate: Decimal = Decimal("0.00")

    def __post_init__(self):
        if isinstance(self.balance, (int, float, str)):
            self.balance = Decimal(str(self.balance))
        if isinstance(self.interest_rate, (int, float, str)):
            self.interest_rate = Decimal(str(self.interest_rate))
        if isinstance(self.account_type, str):
            self.account_type = AccountType(self.account_type)
        if isinstance(self.status, str):
            self.status = AccountStatus(self.status)
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)


@dataclass
class Transaction:
    id: str
    account_id: str
    transaction_type: TransactionType
    amount: Decimal
    balance_after: Decimal
    description: str
    reference: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        if isinstance(self.amount, (int, float, str)):
            self.amount = Decimal(str(self.amount))
        if isinstance(self.balance_after, (int, float, str)):
            self.balance_after = Decimal(str(self.balance_after))
        if isinstance(self.transaction_type, str):
            self.transaction_type = TransactionType(self.transaction_type)
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)


@dataclass
class Statement:
    account: Account
    transactions: List[Transaction]
    from_date: datetime
    to_date: datetime
    opening_balance: Decimal
    closing_balance: Decimal
    total_credits: Decimal
    total_debits: Decimal


# ─── Database Layer ────────────────────────────────────────────────────────────
class BankingDB:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self):
        with self.transaction() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id           TEXT PRIMARY KEY,
                    owner        TEXT NOT NULL,
                    balance      TEXT NOT NULL DEFAULT '0.00',
                    currency     TEXT NOT NULL DEFAULT 'USD',
                    account_type TEXT NOT NULL,
                    status       TEXT NOT NULL DEFAULT 'active',
                    interest_rate TEXT NOT NULL DEFAULT '0.00',
                    created_at   TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    id               TEXT PRIMARY KEY,
                    account_id       TEXT NOT NULL REFERENCES accounts(id),
                    transaction_type TEXT NOT NULL,
                    amount           TEXT NOT NULL,
                    balance_after    TEXT NOT NULL,
                    description      TEXT NOT NULL,
                    reference        TEXT,
                    created_at       TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_txn_account
                    ON transactions(account_id, created_at);

                CREATE INDEX IF NOT EXISTS idx_accounts_owner
                    ON accounts(owner);
            """)

    # ── Account Operations ───────────────────────────────────────────────────

    def create_account(self, account: Account) -> Account:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO accounts
                   (id, owner, balance, currency, account_type, status,
                    interest_rate, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    account.id, account.owner,
                    str(account.balance), account.currency,
                    account.account_type.value, account.status.value,
                    str(account.interest_rate),
                    account.created_at.isoformat(),
                ),
            )
        logger.info("Account created: %s for %s", account.id, account.owner)
        return account

    def get_account(self, account_id: str) -> Optional[Account]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM accounts WHERE id=?", (account_id,)
            ).fetchone()
            return self._row_to_account(row) if row else None
        finally:
            conn.close()

    def update_account_balance(
        self,
        conn: sqlite3.Connection,
        account_id: str,
        new_balance: Decimal,
    ):
        conn.execute(
            "UPDATE accounts SET balance=? WHERE id=?",
            (str(new_balance), account_id),
        )

    def update_account_status(self, account_id: str, status: AccountStatus):
        with self.transaction() as conn:
            conn.execute(
                "UPDATE accounts SET status=? WHERE id=?",
                (status.value, account_id),
            )

    def record_transaction(
        self, conn: sqlite3.Connection, txn: Transaction
    ) -> Transaction:
        conn.execute(
            """INSERT INTO transactions
               (id, account_id, transaction_type, amount, balance_after,
                description, reference, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                txn.id, txn.account_id, txn.transaction_type.value,
                str(txn.amount), str(txn.balance_after),
                txn.description, txn.reference,
                txn.created_at.isoformat(),
            ),
        )
        return txn

    def get_transactions(
        self,
        account_id: str,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        limit: int = 500,
    ) -> List[Transaction]:
        conn = self._connect()
        try:
            query = "SELECT * FROM transactions WHERE account_id=?"
            params: list = [account_id]
            if from_dt:
                query += " AND created_at >= ?"
                params.append(from_dt.isoformat())
            if to_dt:
                query += " AND created_at <= ?"
                params.append(to_dt.isoformat())
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_transaction(r) for r in rows]
        finally:
            conn.close()

    def list_accounts(self, owner: Optional[str] = None) -> List[Account]:
        conn = self._connect()
        try:
            if owner:
                rows = conn.execute(
                    "SELECT * FROM accounts WHERE owner=? ORDER BY created_at",
                    (owner,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM accounts ORDER BY created_at"
                ).fetchall()
            return [self._row_to_account(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    def _row_to_account(row: sqlite3.Row) -> Account:
        return Account(
            id=row["id"],
            owner=row["owner"],
            balance=Decimal(row["balance"]),
            currency=row["currency"],
            account_type=AccountType(row["account_type"]),
            status=AccountStatus(row["status"]),
            interest_rate=Decimal(row["interest_rate"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _row_to_transaction(row: sqlite3.Row) -> Transaction:
        return Transaction(
            id=row["id"],
            account_id=row["account_id"],
            transaction_type=TransactionType(row["transaction_type"]),
            amount=Decimal(row["amount"]),
            balance_after=Decimal(row["balance_after"]),
            description=row["description"],
            reference=row["reference"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


# ─── Banking Service ───────────────────────────────────────────────────────────
class BankingService:
    """High-level banking operations with ACID guarantees."""

    def __init__(self, db: Optional[BankingDB] = None):
        self.db = db or BankingDB()

    def open_account(
        self,
        owner: str,
        initial_deposit: Decimal,
        currency: str = "USD",
        account_type: AccountType = AccountType.CHECKING,
        interest_rate: Decimal = Decimal("0.00"),
    ) -> Account:
        """Open a new bank account with an initial deposit."""
        if initial_deposit < Decimal("0"):
            raise ValueError("Initial deposit cannot be negative")

        account = Account(
            id=str(uuid.uuid4()),
            owner=owner,
            balance=Decimal("0"),
            currency=currency.upper(),
            account_type=account_type,
            interest_rate=interest_rate,
        )
        self.db.create_account(account)

        if initial_deposit > Decimal("0"):
            self._deposit(account.id, initial_deposit, "Initial deposit")

        return self.db.get_account(account.id)

    def _deposit(
        self,
        account_id: str,
        amount: Decimal,
        description: str,
        txn_type: TransactionType = TransactionType.DEPOSIT,
        reference: Optional[str] = None,
    ) -> Transaction:
        amount = amount.quantize(PRECISION, rounding=ROUND_HALF_UP)
        if amount <= Decimal("0"):
            raise ValueError("Deposit amount must be positive")

        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT balance, status FROM accounts WHERE id=? FOR UPDATE" 
                if False else "SELECT balance, status FROM accounts WHERE id=?",
                (account_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Account {account_id} not found")
            if row["status"] == AccountStatus.FROZEN.value:
                raise ValueError(f"Account {account_id} is frozen")

            new_balance = Decimal(row["balance"]) + amount
            self.db.update_account_balance(conn, account_id, new_balance)
            txn = Transaction(
                id=str(uuid.uuid4()),
                account_id=account_id,
                transaction_type=txn_type,
                amount=amount,
                balance_after=new_balance,
                description=description,
                reference=reference,
            )
            return self.db.record_transaction(conn, txn)

    def _withdraw(
        self,
        account_id: str,
        amount: Decimal,
        description: str,
        txn_type: TransactionType = TransactionType.WITHDRAWAL,
        reference: Optional[str] = None,
    ) -> Transaction:
        amount = amount.quantize(PRECISION, rounding=ROUND_HALF_UP)
        if amount <= Decimal("0"):
            raise ValueError("Withdrawal amount must be positive")

        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT balance, status, account_type FROM accounts WHERE id=?",
                (account_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Account {account_id} not found")
            if row["status"] == AccountStatus.FROZEN.value:
                raise ValueError(f"Account {account_id} is frozen")

            current_balance = Decimal(row["balance"])
            account_type = AccountType(row["account_type"])

            # Credit accounts allow negative; others require sufficient balance
            if account_type != AccountType.CREDIT and current_balance < amount:
                raise ValueError(
                    f"Insufficient funds: balance={current_balance}, requested={amount}"
                )

            new_balance = current_balance - amount
            self.db.update_account_balance(conn, account_id, new_balance)
            txn = Transaction(
                id=str(uuid.uuid4()),
                account_id=account_id,
                transaction_type=txn_type,
                amount=amount,
                balance_after=new_balance,
                description=description,
                reference=reference,
            )
            return self.db.record_transaction(conn, txn)

    def transfer(
        self,
        from_account_id: str,
        to_account_id: str,
        amount: Decimal,
        description: str = "Transfer",
    ) -> tuple[Transaction, Transaction]:
        """Atomic transfer between two accounts."""
        amount = Decimal(str(amount)).quantize(PRECISION, rounding=ROUND_HALF_UP)
        if amount <= Decimal("0"):
            raise ValueError("Transfer amount must be positive")
        if from_account_id == to_account_id:
            raise ValueError("Cannot transfer to same account")

        reference = str(uuid.uuid4())

        # Use deterministic ordering to prevent deadlocks
        acct_ids = sorted([from_account_id, to_account_id])

        with self.db.transaction() as conn:
            rows = {
                r["id"]: r
                for r in conn.execute(
                    "SELECT id, balance, status, currency FROM accounts WHERE id IN (?,?)",
                    acct_ids,
                ).fetchall()
            }
            if from_account_id not in rows:
                raise ValueError(f"Source account {from_account_id} not found")
            if to_account_id not in rows:
                raise ValueError(f"Destination account {to_account_id} not found")

            from_row = rows[from_account_id]
            to_row = rows[to_account_id]

            if from_row["status"] == AccountStatus.FROZEN.value:
                raise ValueError("Source account is frozen")
            if to_row["status"] == AccountStatus.FROZEN.value:
                raise ValueError("Destination account is frozen")
            if from_row["currency"] != to_row["currency"]:
                raise ValueError("Cross-currency transfers not supported")

            from_balance = Decimal(from_row["balance"])
            to_balance = Decimal(to_row["balance"])

            if from_balance < amount:
                raise ValueError(
                    f"Insufficient funds: available={from_balance}, requested={amount}"
                )

            new_from = from_balance - amount
            new_to = to_balance + amount

            self.db.update_account_balance(conn, from_account_id, new_from)
            self.db.update_account_balance(conn, to_account_id, new_to)

            out_txn = self.db.record_transaction(
                conn,
                Transaction(
                    id=str(uuid.uuid4()),
                    account_id=from_account_id,
                    transaction_type=TransactionType.TRANSFER_OUT,
                    amount=amount,
                    balance_after=new_from,
                    description=f"{description} → {to_account_id[:8]}",
                    reference=reference,
                ),
            )
            in_txn = self.db.record_transaction(
                conn,
                Transaction(
                    id=str(uuid.uuid4()),
                    account_id=to_account_id,
                    transaction_type=TransactionType.TRANSFER_IN,
                    amount=amount,
                    balance_after=new_to,
                    description=f"{description} ← {from_account_id[:8]}",
                    reference=reference,
                ),
            )

        logger.info("Transfer %s: %s → %s, amount=%s", reference, from_account_id[:8], to_account_id[:8], amount)
        return out_txn, in_txn

    def get_balance(self, account_id: str) -> Decimal:
        """Return current account balance."""
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")
        return account.balance

    def get_statement(self, account_id: str, days: int = 30) -> Statement:
        """Generate account statement for the past N days."""
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")

        to_dt = datetime.utcnow()
        from_dt = to_dt - timedelta(days=days)

        transactions = self.db.get_transactions(account_id, from_dt, to_dt)
        transactions_sorted = sorted(transactions, key=lambda t: t.created_at)

        total_credits = sum(
            t.amount for t in transactions_sorted
            if t.transaction_type in (
                TransactionType.DEPOSIT,
                TransactionType.TRANSFER_IN,
                TransactionType.INTEREST,
            )
        )
        total_debits = sum(
            t.amount for t in transactions_sorted
            if t.transaction_type in (
                TransactionType.WITHDRAWAL,
                TransactionType.TRANSFER_OUT,
                TransactionType.FEE,
            )
        )

        opening_balance = (
            transactions_sorted[0].balance_after - transactions_sorted[0].amount
            if transactions_sorted and transactions_sorted[0].transaction_type
            not in (TransactionType.WITHDRAWAL, TransactionType.TRANSFER_OUT)
            else account.balance + total_debits - total_credits
        )

        return Statement(
            account=account,
            transactions=transactions_sorted,
            from_date=from_dt,
            to_date=to_dt,
            opening_balance=opening_balance,
            closing_balance=account.balance,
            total_credits=Decimal(str(total_credits)),
            total_debits=Decimal(str(total_debits)),
        )

    def apply_interest(self, account_id: str, rate: Optional[Decimal] = None) -> Transaction:
        """Apply interest to a savings or money market account."""
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")
        if account.account_type not in (AccountType.SAVINGS, AccountType.MONEY_MARKET):
            raise ValueError("Interest only applies to savings/money market accounts")
        if account.status != AccountStatus.ACTIVE:
            raise ValueError("Account must be active to receive interest")

        effective_rate = rate if rate is not None else account.interest_rate
        if effective_rate <= Decimal("0"):
            raise ValueError("Interest rate must be positive")

        interest_amount = (account.balance * effective_rate).quantize(
            PRECISION, rounding=ROUND_HALF_UP
        )
        return self._deposit(
            account_id,
            interest_amount,
            f"Interest at {effective_rate:.4%}",
            txn_type=TransactionType.INTEREST,
        )

    def freeze_account(self, account_id: str) -> Account:
        """Freeze an account to prevent transactions."""
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")
        self.db.update_account_status(account_id, AccountStatus.FROZEN)
        logger.warning("Account frozen: %s (owner=%s)", account_id, account.owner)
        return self.db.get_account(account_id)

    def unfreeze_account(self, account_id: str) -> Account:
        """Unfreeze a previously frozen account."""
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")
        self.db.update_account_status(account_id, AccountStatus.ACTIVE)
        logger.info("Account unfrozen: %s", account_id)
        return self.db.get_account(account_id)

    def close_account(self, account_id: str) -> Account:
        """Close an account (balance must be zero)."""
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")
        if account.balance != Decimal("0"):
            raise ValueError(
                f"Cannot close account with non-zero balance: {account.balance}"
            )
        self.db.update_account_status(account_id, AccountStatus.CLOSED)
        return self.db.get_account(account_id)

    def export_statement_csv(self, account_id: str, days: int = 30) -> str:
        """Export statement as CSV string."""
        statement = self.get_statement(account_id, days)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Date", "Type", "Description", "Amount", "Balance", "Reference"
        ])
        for txn in statement.transactions:
            writer.writerow([
                txn.created_at.isoformat(),
                txn.transaction_type.value,
                txn.description,
                str(txn.amount),
                str(txn.balance_after),
                txn.reference or "",
            ])
        return output.getvalue()


# ─── CLI ───────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="banking",
        description="BlackRoad Banking Platform CLI",
    )
    parser.add_argument("--db", default=str(DB_PATH), help="SQLite database path")
    sub = parser.add_subparsers(dest="command", required=True)

    # open-account
    p_open = sub.add_parser("open", help="Open a new account")
    p_open.add_argument("owner")
    p_open.add_argument("deposit", type=str, help="Initial deposit amount")
    p_open.add_argument("--currency", default="USD")
    p_open.add_argument(
        "--type",
        dest="account_type",
        choices=[t.value for t in AccountType],
        default=AccountType.CHECKING.value,
    )
    p_open.add_argument("--rate", default="0.00", help="Annual interest rate (decimal)")

    # balance
    p_bal = sub.add_parser("balance", help="Check account balance")
    p_bal.add_argument("account_id")

    # transfer
    p_xfer = sub.add_parser("transfer", help="Transfer between accounts")
    p_xfer.add_argument("from_id")
    p_xfer.add_argument("to_id")
    p_xfer.add_argument("amount")

    # statement
    p_stmt = sub.add_parser("statement", help="View account statement")
    p_stmt.add_argument("account_id")
    p_stmt.add_argument("--days", type=int, default=30)
    p_stmt.add_argument("--csv", action="store_true")

    # interest
    p_int = sub.add_parser("interest", help="Apply interest to account")
    p_int.add_argument("account_id")
    p_int.add_argument("--rate", default=None)

    # freeze
    p_frz = sub.add_parser("freeze", help="Freeze an account")
    p_frz.add_argument("account_id")

    # list
    p_list = sub.add_parser("list", help="List accounts")
    p_list.add_argument("--owner", default=None)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    db = BankingDB(Path(args.db))
    svc = BankingService(db)

    if args.command == "open":
        acct = svc.open_account(
            owner=args.owner,
            initial_deposit=Decimal(args.deposit),
            currency=args.currency,
            account_type=AccountType(args.account_type),
            interest_rate=Decimal(args.rate),
        )
        print(f"✓ Account opened: {acct.id}")
        print(f"  Owner:    {acct.owner}")
        print(f"  Balance:  {acct.balance} {acct.currency}")
        print(f"  Type:     {acct.account_type.value}")

    elif args.command == "balance":
        bal = svc.get_balance(args.account_id)
        print(f"Balance: {bal}")

    elif args.command == "transfer":
        out_txn, _ = svc.transfer(args.from_id, args.to_id, Decimal(args.amount))
        print(f"✓ Transfer complete (ref={out_txn.reference[:8]})")

    elif args.command == "statement":
        if args.csv:
            print(svc.export_statement_csv(args.account_id, args.days))
        else:
            stmt = svc.get_statement(args.account_id, args.days)
            print(f"\n{'='*60}")
            print(f"  ACCOUNT STATEMENT — {stmt.account.id[:8]}")
            print(f"  Owner: {stmt.account.owner}")
            print(f"  Period: {stmt.from_date.date()} to {stmt.to_date.date()}")
            print(f"{'='*60}")
            print(f"  Opening Balance: {stmt.opening_balance:>12}")
            print(f"  Total Credits:   {stmt.total_credits:>12}")
            print(f"  Total Debits:    {stmt.total_debits:>12}")
            print(f"  Closing Balance: {stmt.closing_balance:>12}")
            print(f"{'─'*60}")
            for t in stmt.transactions:
                sign = "+" if t.transaction_type in (
                    TransactionType.DEPOSIT, TransactionType.TRANSFER_IN,
                    TransactionType.INTEREST
                ) else "-"
                print(
                    f"  {t.created_at.strftime('%Y-%m-%d %H:%M')}  "
                    f"{sign}{t.amount:>10}  {t.description[:30]:<30}  "
                    f"bal={t.balance_after}"
                )

    elif args.command == "interest":
        rate = Decimal(args.rate) if args.rate else None
        txn = svc.apply_interest(args.account_id, rate)
        print(f"✓ Interest applied: +{txn.amount}")

    elif args.command == "freeze":
        acct = svc.freeze_account(args.account_id)
        print(f"✓ Account {acct.id[:8]} is now {acct.status.value}")

    elif args.command == "list":
        accounts = svc.db.list_accounts(args.owner)
        for a in accounts:
            print(
                f"  {a.id[:8]}  {a.owner:<20}  {a.balance:>12} {a.currency}  "
                f"{a.account_type.value:<12}  {a.status.value}"
            )


if __name__ == "__main__":
    main()
