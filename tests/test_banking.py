"""Tests for BlackRoad Banking Platform."""

import pytest
from decimal import Decimal
from datetime import datetime
import tempfile
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from banking_platform import (
    BankingDB, BankingService, Account, AccountType, AccountStatus,
    TransactionType, Transaction
)


@pytest.fixture
def svc(tmp_path):
    db = BankingDB(tmp_path / "test.db")
    return BankingService(db)


def test_open_account_basic(svc):
    acct = svc.open_account("Alice", Decimal("1000.00"))
    assert acct.owner == "Alice"
    assert acct.balance == Decimal("1000.00")
    assert acct.status == AccountStatus.ACTIVE
    assert acct.account_type == AccountType.CHECKING
    assert acct.currency == "USD"


def test_open_account_with_type_and_currency(svc):
    acct = svc.open_account(
        "Bob", Decimal("500"), currency="EUR",
        account_type=AccountType.SAVINGS,
        interest_rate=Decimal("0.045")
    )
    assert acct.currency == "EUR"
    assert acct.account_type == AccountType.SAVINGS
    assert acct.interest_rate == Decimal("0.045")


def test_open_account_negative_deposit_raises(svc):
    with pytest.raises(ValueError, match="negative"):
        svc.open_account("X", Decimal("-10"))


def test_get_balance(svc):
    acct = svc.open_account("Carol", Decimal("750"))
    bal = svc.get_balance(acct.id)
    assert bal == Decimal("750.00")


def test_get_balance_nonexistent_raises(svc):
    with pytest.raises(ValueError, match="not found"):
        svc.get_balance("nonexistent-id")


def test_transfer_basic(svc):
    a = svc.open_account("Dave", Decimal("1000"))
    b = svc.open_account("Eve", Decimal("200"))
    out, inn = svc.transfer(a.id, b.id, Decimal("300"))
    assert svc.get_balance(a.id) == Decimal("700.00")
    assert svc.get_balance(b.id) == Decimal("500.00")
    assert out.transaction_type == TransactionType.TRANSFER_OUT
    assert inn.transaction_type == TransactionType.TRANSFER_IN
    assert out.reference == inn.reference


def test_transfer_insufficient_funds(svc):
    a = svc.open_account("Frank", Decimal("100"))
    b = svc.open_account("Grace", Decimal("0"))
    with pytest.raises(ValueError, match="Insufficient"):
        svc.transfer(a.id, b.id, Decimal("500"))


def test_transfer_same_account_raises(svc):
    a = svc.open_account("Hank", Decimal("100"))
    with pytest.raises(ValueError, match="same account"):
        svc.transfer(a.id, a.id, Decimal("50"))


def test_transfer_frozen_account_raises(svc):
    a = svc.open_account("Ivan", Decimal("1000"))
    b = svc.open_account("Judy", Decimal("0"))
    svc.freeze_account(a.id)
    with pytest.raises(ValueError, match="frozen"):
        svc.transfer(a.id, b.id, Decimal("100"))


def test_freeze_and_unfreeze(svc):
    acct = svc.open_account("Karl", Decimal("500"))
    frozen = svc.freeze_account(acct.id)
    assert frozen.status == AccountStatus.FROZEN
    active = svc.unfreeze_account(acct.id)
    assert active.status == AccountStatus.ACTIVE


def test_apply_interest(svc):
    acct = svc.open_account(
        "Lily", Decimal("10000"),
        account_type=AccountType.SAVINGS,
        interest_rate=Decimal("0.05"),
    )
    txn = svc.apply_interest(acct.id)
    assert txn.transaction_type == TransactionType.INTEREST
    assert txn.amount == Decimal("500.00")
    assert svc.get_balance(acct.id) == Decimal("10500.00")


def test_apply_interest_wrong_type(svc):
    acct = svc.open_account("Mike", Decimal("1000"), account_type=AccountType.CHECKING)
    with pytest.raises(ValueError, match="savings"):
        svc.apply_interest(acct.id, Decimal("0.05"))


def test_get_statement(svc):
    a = svc.open_account("Nancy", Decimal("2000"))
    b = svc.open_account("Oscar", Decimal("0"))
    svc.transfer(a.id, b.id, Decimal("500"))
    stmt = svc.get_statement(a.id, days=30)
    assert stmt.closing_balance == Decimal("1500.00")
    assert stmt.total_debits >= Decimal("500.00")
    assert len(stmt.transactions) >= 2  # deposit + transfer_out


def test_close_account_zero_balance(svc):
    a = svc.open_account("Pete", Decimal("0"))
    closed = svc.close_account(a.id)
    assert closed.status == AccountStatus.CLOSED


def test_close_account_nonzero_balance_raises(svc):
    a = svc.open_account("Quinn", Decimal("100"))
    with pytest.raises(ValueError, match="non-zero balance"):
        svc.close_account(a.id)


def test_export_statement_csv(svc):
    a = svc.open_account("Rose", Decimal("1000"))
    csv_output = svc.export_statement_csv(a.id, days=30)
    lines = csv_output.strip().split("\n")
    assert "Date" in lines[0]
    assert len(lines) >= 2  # header + at least 1 transaction


def test_list_accounts_by_owner(svc):
    svc.open_account("Sam", Decimal("100"))
    svc.open_account("Sam", Decimal("200"), account_type=AccountType.SAVINGS)
    svc.open_account("Tara", Decimal("300"))
    sam_accounts = svc.db.list_accounts("Sam")
    assert len(sam_accounts) == 2
    assert all(a.owner == "Sam" for a in sam_accounts)


def test_transfer_atomicity_on_error(svc):
    """Transfer should be atomic - no partial update on failure."""
    a = svc.open_account("Uma", Decimal("50"))
    b = svc.open_account("Vera", Decimal("0"))
    try:
        svc.transfer(a.id, b.id, Decimal("100"))
    except ValueError:
        pass
    assert svc.get_balance(a.id) == Decimal("50.00")
    assert svc.get_balance(b.id) == Decimal("0.00")


def test_decimal_precision(svc):
    """Ensure no floating point errors."""
    a = svc.open_account("Will", Decimal("1000.00"))
    b = svc.open_account("Xena", Decimal("0.00"))
    svc.transfer(a.id, b.id, Decimal("333.33"))
    svc.transfer(a.id, b.id, Decimal("333.33"))
    svc.transfer(a.id, b.id, Decimal("333.34"))
    assert svc.get_balance(a.id) == Decimal("0.00")
