"""Tests for BlackRoad Banking Platform."""
import os
import tempfile
import pytest
from banking import (
    init_db, open_account, get_account, get_balance, transfer,
    deposit, withdraw, get_statement, calculate_interest,
    freeze_account, unfreeze_account, detect_anomaly, list_accounts,
    list_transactions, get_audit_log,
)


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "test_banking.db")
    init_db(path)
    return path


def test_open_account_creates_account(db):
    acc = open_account("Alice", "checking", 1000.0, path=db)
    assert acc.owner == "Alice"
    assert acc.type == "checking"
    assert acc.balance == 1000.0
    assert acc.currency == "USD"
    assert not acc.frozen
    assert len(acc.iban) > 10
    assert len(acc.routing) == 9


def test_open_account_savings(db):
    acc = open_account("Bob", "savings", 500.0, interest_rate=0.05, path=db)
    assert acc.type == "savings"
    assert acc.interest_rate == 0.05


def test_open_account_investment(db):
    acc = open_account("Carol", "investment", 10000.0, path=db)
    assert acc.type == "investment"


def test_open_account_invalid_type(db):
    with pytest.raises(ValueError):
        open_account("Dave", "mortgage", path=db)


def test_open_account_negative_deposit(db):
    with pytest.raises(ValueError):
        open_account("Eve", "checking", -100.0, path=db)


def test_get_balance(db):
    acc = open_account("Frank", "checking", 750.0, path=db)
    assert get_balance(acc.id, db) == 750.0


def test_deposit(db):
    acc = open_account("Grace", "checking", 0.0, path=db)
    txn = deposit(acc.id, 200.0, path=db)
    assert txn.amount == 200.0
    assert txn.type == "credit"
    assert get_balance(acc.id, db) == 200.0


def test_withdraw(db):
    acc = open_account("Hank", "checking", 500.0, path=db)
    txn = withdraw(acc.id, 100.0, path=db)
    assert txn.amount == 100.0
    assert txn.type == "debit"
    assert get_balance(acc.id, db) == 400.0


def test_withdraw_insufficient_funds(db):
    acc = open_account("Iris", "checking", 50.0, path=db)
    with pytest.raises(ValueError, match="Insufficient"):
        withdraw(acc.id, 100.0, path=db)


def test_transfer_moves_funds(db):
    src = open_account("Jack", "checking", 1000.0, path=db)
    dst = open_account("Jill", "checking", 0.0, path=db)
    debit, credit = transfer(src.id, dst.id, 200.0, "test", fee_rate=0.0, path=db)
    assert get_balance(src.id, db) == 800.0
    assert get_balance(dst.id, db) == 200.0
    assert debit.reference == credit.reference


def test_transfer_with_fee(db):
    src = open_account("Ken", "checking", 1000.0, path=db)
    dst = open_account("Lia", "checking", 0.0, path=db)
    transfer(src.id, dst.id, 100.0, fee_rate=0.01, path=db)
    assert get_balance(src.id, db) == pytest.approx(899.0)
    assert get_balance(dst.id, db) == 100.0


def test_transfer_insufficient_funds(db):
    src = open_account("Mia", "checking", 10.0, path=db)
    dst = open_account("Ned", "checking", 0.0, path=db)
    with pytest.raises(ValueError, match="Insufficient"):
        transfer(src.id, dst.id, 100.0, path=db)


def test_transfer_unknown_source(db):
    dst = open_account("Olive", "checking", 0.0, path=db)
    with pytest.raises(KeyError):
        transfer("nonexistent", dst.id, 50.0, path=db)


def test_freeze_prevents_transfer(db):
    src = open_account("Pat", "checking", 1000.0, path=db)
    dst = open_account("Quinn", "checking", 0.0, path=db)
    freeze_account(src.id, path=db)
    with pytest.raises(PermissionError):
        transfer(src.id, dst.id, 100.0, path=db)


def test_unfreeze_allows_transfer(db):
    src = open_account("Rose", "checking", 1000.0, path=db)
    dst = open_account("Sam", "checking", 0.0, path=db)
    freeze_account(src.id, path=db)
    unfreeze_account(src.id, path=db)
    transfer(src.id, dst.id, 100.0, fee_rate=0.0, path=db)
    assert get_balance(src.id, db) == 900.0


def test_get_statement(db):
    acc = open_account("Tina", "checking", 500.0, path=db)
    deposit(acc.id, 100.0, path=db)
    txns = get_statement(acc.id, "2000-01-01", "2099-12-31", db)
    assert len(txns) >= 2


def test_calculate_interest(db):
    acc = open_account("Uma", "savings", 10000.0, interest_rate=0.05, path=db)
    interest = calculate_interest(acc.id, path=db)
    expected = round(10000.0 * (0.05 / 365), 4)
    assert interest == pytest.approx(expected, rel=1e-4)
    assert get_balance(acc.id, db) == pytest.approx(10000.0 + interest, rel=1e-4)


def test_calculate_interest_zero_rate(db):
    acc = open_account("Vera", "checking", 1000.0, path=db)
    interest = calculate_interest(acc.id, path=db)
    assert interest == 0.0


def test_detect_anomaly(db):
    acc = open_account("Will", "checking", 100000.0, path=db)
    dst = open_account("Xena", "checking", 0.0, path=db)
    # Normal transfers
    for amount in [100, 105, 110, 95, 102, 98, 103]:
        transfer(acc.id, dst.id, amount, fee_rate=0.0, path=db)
    deposit(acc.id, 100000.0, path=db)
    # Anomalous transfer
    transfer(acc.id, dst.id, 50000.0, fee_rate=0.0, path=db)
    flagged = detect_anomaly(acc.id, path=db)
    assert len(flagged) >= 1


def test_detect_anomaly_not_enough_data(db):
    acc = open_account("Yara", "checking", 1000.0, path=db)
    flagged = detect_anomaly(acc.id, path=db)
    assert flagged == []


def test_list_accounts(db):
    open_account("Zara", "checking", 100.0, path=db)
    open_account("Aaron", "savings", 200.0, path=db)
    accounts = list_accounts(db)
    assert len(accounts) >= 2


def test_list_transactions(db):
    acc = open_account("Beth", "checking", 500.0, path=db)
    deposit(acc.id, 100.0, path=db)
    txns = list_transactions(acc.id, limit=10, path=db)
    assert len(txns) >= 1


def test_audit_log(db):
    acc = open_account("Cal", "checking", 100.0, path=db)
    freeze_account(acc.id, "admin", db)
    unfreeze_account(acc.id, "admin", db)
    logs = get_audit_log(acc.id, db)
    actions = [l.action for l in logs]
    assert "freeze" in actions
    assert "unfreeze" in actions


def test_frozen_deposit_blocked(db):
    acc = open_account("Dana", "checking", 100.0, path=db)
    freeze_account(acc.id, path=db)
    with pytest.raises(PermissionError):
        deposit(acc.id, 50.0, path=db)


def test_frozen_withdraw_blocked(db):
    acc = open_account("Eli", "checking", 200.0, path=db)
    freeze_account(acc.id, path=db)
    with pytest.raises(PermissionError):
        withdraw(acc.id, 50.0, path=db)
