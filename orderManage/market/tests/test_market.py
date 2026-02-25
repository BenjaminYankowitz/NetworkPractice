"""
Unit tests for market/mockMarket.py.

The conftest.py in this directory pre-stubs psycopg_pool, pika, and
marketMessages_pb2 so the module can be imported without running services.
"""
import sys
import psycopg
import pytest
from unittest.mock import MagicMock, patch, call

import mockMarket
from mockMarket import match_orders


# ---------------------------------------------------------------------------
# Helper: build an existing-order dict as the DB would return.
# ---------------------------------------------------------------------------

def _order(id, owner, price, quantity, filled=0):
    return {"id": id, "owner": owner, "price": price, "quantity": quantity, "filled": filled}


# ---------------------------------------------------------------------------
# match_orders — no-match cases
# ---------------------------------------------------------------------------

def test_buy_no_match():
    fills, amount_filled, total_cost = match_orders([], "AAPL", True, 1000, 10)
    assert fills == []
    assert amount_filled == 0
    assert total_cost == 0


def test_sell_no_match():
    fills, amount_filled, total_cost = match_orders([], "AAPL", False, 1000, 10)
    assert fills == []
    assert amount_filled == 0
    assert total_cost == 0


# ---------------------------------------------------------------------------
# match_orders — exact matches
# ---------------------------------------------------------------------------

def test_buy_exact_match_one_order():
    existing = [_order(id=1, owner=100, price=900, quantity=10)]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1000, 10)
    assert fills == [(1, 100, 10)]
    assert amount_filled == 10
    assert total_cost == 9000  # 10 * 900


def test_sell_exact_match_one_order():
    existing = [_order(id=2, owner=200, price=1100, quantity=10)]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", False, 1000, 10)
    assert fills == [(2, 200, 10)]
    assert amount_filled == 10
    assert total_cost == 11000  # 10 * 1100


# ---------------------------------------------------------------------------
# match_orders — partial fills
# ---------------------------------------------------------------------------

def test_buy_partial_fill_incoming():
    """Incoming buy of 10, only 5 available to sell → incoming rests with 5 unfilled."""
    existing = [_order(id=1, owner=100, price=900, quantity=5)]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1000, 10)
    assert fills == [(1, 100, 5)]
    assert amount_filled == 5  # only 5 filled; 5 remain for the incoming order
    assert total_cost == 5 * 900


def test_buy_partial_fill_resting():
    """Incoming buy of 10, sell has 15 → sell partially consumed (5 remain)."""
    existing = [_order(id=1, owner=100, price=900, quantity=15)]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1000, 10)
    assert fills == [(1, 100, 10)]
    assert amount_filled == 10  # incoming fully filled
    assert total_cost == 10 * 900


# ---------------------------------------------------------------------------
# match_orders — multi-order match
# ---------------------------------------------------------------------------

def test_buy_multi_order_match():
    """Buy 10 matches two sells of 5 each → both fully consumed."""
    existing = [
        _order(id=1, owner=101, price=800, quantity=5),
        _order(id=2, owner=102, price=900, quantity=5),
    ]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1000, 10)
    assert fills == [(1, 101, 5), (2, 102, 5)]
    assert amount_filled == 10
    assert total_cost == 4000 + 4500  # 5*800 + 5*900


# ---------------------------------------------------------------------------
# match_orders — price priority
# ---------------------------------------------------------------------------

def test_price_priority_buy():
    """For a buy order, existing sells must be matched lowest price first."""
    # SQL returns sells ASC by price for a buy; we replicate that ordering here.
    existing = [
        _order(id=3, owner=103, price=1000, quantity=5),  # cheapest
        _order(id=2, owner=102, price=1100, quantity=5),
        _order(id=1, owner=101, price=1200, quantity=5),  # most expensive
    ]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1500, 5)
    assert amount_filled == 5
    assert fills[0] == (3, 103, 5)  # lowest-price sell matched first
    assert total_cost == 5 * 1000


def test_price_priority_sell():
    """For a sell order, existing buys must be matched highest price first."""
    # SQL returns buys DESC by price for a sell; we replicate that ordering here.
    existing = [
        _order(id=3, owner=103, price=1200, quantity=5),  # highest
        _order(id=2, owner=102, price=1100, quantity=5),
        _order(id=1, owner=101, price=1000, quantity=5),  # lowest
    ]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", False, 900, 5)
    assert amount_filled == 5
    assert fills[0] == (3, 103, 5)  # highest-price buy matched first
    assert total_cost == 5 * 1200


# ---------------------------------------------------------------------------
# match_orders — prior fills and early stop
# ---------------------------------------------------------------------------

def test_partially_filled_resting_order():
    """Existing order with prior fills should only contribute its remaining quantity."""
    existing = [_order(id=1, owner=100, price=900, quantity=10, filled=6)]  # 4 left
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1000, 10)
    assert fills == [(1, 100, 4)]
    assert amount_filled == 4
    assert total_cost == 4 * 900


def test_incoming_order_stops_early():
    """Incoming order of 3 against two sells of 5 each: only the first is touched."""
    existing = [
        _order(id=1, owner=101, price=800, quantity=5),
        _order(id=2, owner=102, price=900, quantity=5),
    ]
    fills, amount_filled, total_cost = match_orders(existing, "AAPL", True, 1000, 3)
    assert fills == [(1, 101, 3)]
    assert amount_filled == 3
    assert total_cost == 3 * 800


# ---------------------------------------------------------------------------
# on_signup — tests using mocked pool.connection()
# ---------------------------------------------------------------------------

def _make_mock_cursor(fetchone_result=None, execute_side_effect=None):
    """Return a configured mock cursor context manager."""
    mock_cur = MagicMock()
    if fetchone_result is not None:
        mock_cur.fetchone.return_value = fetchone_result
    if execute_side_effect is not None:
        mock_cur.execute.side_effect = execute_side_effect
    mock_cursor_cm = MagicMock()
    mock_cursor_cm.__enter__ = MagicMock(return_value=mock_cur)
    mock_cursor_cm.__exit__ = MagicMock(return_value=False)
    return mock_cursor_cm, mock_cur


def _make_mock_pool_connection(mock_cursor_cm):
    """Return a pool.connection() context manager that yields a conn with cursor()."""
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor_cm
    mock_conn_cm = MagicMock()
    mock_conn_cm.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn_cm.__exit__ = MagicMock(return_value=False)
    return mock_conn_cm


def test_signup_new_user():
    """New username → assignedId is set to the DB-returned id."""
    mock_response = MagicMock()
    sys.modules["marketMessages_pb2"].SignupResponseMSG.return_value = mock_response

    mock_cursor_cm, mock_cur = _make_mock_cursor(fetchone_result={"id": 42})
    mock_conn_cm = _make_mock_pool_connection(mock_cursor_cm)

    ch = MagicMock()
    method = MagicMock()
    props = MagicMock()
    props.reply_to = "test-reply-queue"

    with patch.object(mockMarket.pool, "connection", return_value=mock_conn_cm):
        mockMarket.on_signup(ch, method, props, b"fake-proto-bytes")

    assert mock_response.assignedId == 42
    ch.basic_publish.assert_called_once()
    ch.basic_ack.assert_called_once()


def test_signup_duplicate():
    """Duplicate username → UniqueViolation caught, assignedId set to 0."""
    mock_response = MagicMock()
    sys.modules["marketMessages_pb2"].SignupResponseMSG.return_value = mock_response

    mock_cursor_cm, _ = _make_mock_cursor(
        execute_side_effect=psycopg.errors.UniqueViolation()
    )
    mock_conn_cm = _make_mock_pool_connection(mock_cursor_cm)

    ch = MagicMock()
    method = MagicMock()
    props = MagicMock()
    props.reply_to = "test-reply-queue"

    with patch.object(mockMarket.pool, "connection", return_value=mock_conn_cm):
        mockMarket.on_signup(ch, method, props, b"fake-proto-bytes")

    assert mock_response.assignedId == 0
    ch.basic_publish.assert_called_once()
    ch.basic_ack.assert_called_once()


def test_signup_no_reply_to():
    """Missing reply_to → message is acked and no publish is sent."""
    ch = MagicMock()
    method = MagicMock()
    props = MagicMock()
    props.reply_to = None

    mockMarket.on_signup(ch, method, props, b"fake-proto-bytes")

    ch.basic_ack.assert_called_once()
    ch.basic_publish.assert_not_called()


# ---------------------------------------------------------------------------
# on_order — tests using mocked pool.connection() and protobuf stubs
# ---------------------------------------------------------------------------

def _setup_order_msg(symbol, buyside, price, quantity):
    """Configure the mocked OrderMSG instance with the given field values."""
    msg_instance = sys.modules["marketMessages_pb2"].OrderMSG.return_value
    msg_instance.symbol = symbol
    msg_instance.buySide = buyside
    msg_instance.price = price
    msg_instance.quantity = quantity
    return msg_instance


def _make_order_cursor(fetchall_result, fetchone_result=None):
    """Return a mock cursor configured for on_order: fetchall + optional fetchone."""
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = fetchall_result
    if fetchone_result is not None:
        mock_cur.fetchone.return_value = fetchone_result
    mock_cursor_cm = MagicMock()
    mock_cursor_cm.__enter__ = MagicMock(return_value=mock_cur)
    mock_cursor_cm.__exit__ = MagicMock(return_value=False)
    return mock_cursor_cm, mock_cur


def test_on_order_no_match_creates_resting_order():
    """No existing orders → no fills, incoming order inserted as resting order."""
    mock_response = MagicMock()
    sys.modules["marketMessages_pb2"].OrderResponseMSG.return_value = mock_response

    _setup_order_msg("AAPL", True, 1000, 10)

    mock_cursor_cm, mock_cur = _make_order_cursor(
        fetchall_result=[], fetchone_result={"id": 99}
    )
    mock_conn_cm = _make_mock_pool_connection(mock_cursor_cm)

    ch = MagicMock()
    method = MagicMock()
    props = MagicMock()
    props.reply_to = "42"
    props.correlation_id = "corr-1"

    with patch.object(mockMarket.pool, "connection", return_value=mock_conn_cm):
        mockMarket.on_order(ch, method, props, b"fake-order-bytes")

    assert mock_response.amountFilled == 0
    assert mock_response.orderID == 99
    assert mock_response.successful is True
    # Only one publish: the order response (no fills)
    assert ch.basic_publish.call_count == 1
    ch.basic_ack.assert_called_once()
    # INSERT was executed for the resting order
    execute_calls = [str(c) for c in mock_cur.execute.call_args_list]
    assert any("INSERT" in c for c in execute_calls)


def test_on_order_full_fill_incoming():
    """Existing sell exactly matches incoming buy → full fill, no resting order."""
    mock_response = MagicMock()
    sys.modules["marketMessages_pb2"].OrderResponseMSG.return_value = mock_response

    _setup_order_msg("AAPL", True, 1000, 10)

    existing = [_order(id=1, owner=101, price=900, quantity=10)]
    mock_cursor_cm, mock_cur = _make_order_cursor(fetchall_result=existing)
    mock_conn_cm = _make_mock_pool_connection(mock_cursor_cm)

    ch = MagicMock()
    method = MagicMock()
    props = MagicMock()
    props.reply_to = "42"
    props.correlation_id = "corr-2"

    with patch.object(mockMarket.pool, "connection", return_value=mock_conn_cm):
        mockMarket.on_order(ch, method, props, b"fake-order-bytes")

    assert mock_response.amountFilled == 10
    assert mock_response.successful is True
    # Two publishes: one fill notification + one order response
    assert ch.basic_publish.call_count == 2
    ch.basic_ack.assert_called_once()
    # DELETE executed for fully consumed sell; no INSERT
    execute_calls = [str(c) for c in mock_cur.execute.call_args_list]
    assert any("DELETE" in c for c in execute_calls)
    assert not any("INSERT" in c for c in execute_calls)


def test_on_order_partial_fill_incoming():
    """Only 5 of 10 available → incoming buy partially filled, rests with orderID=99."""
    mock_response = MagicMock()
    sys.modules["marketMessages_pb2"].OrderResponseMSG.return_value = mock_response

    _setup_order_msg("AAPL", True, 1000, 10)

    existing = [_order(id=1, owner=101, price=900, quantity=5)]
    mock_cursor_cm, mock_cur = _make_order_cursor(
        fetchall_result=existing, fetchone_result={"id": 99}
    )
    mock_conn_cm = _make_mock_pool_connection(mock_cursor_cm)

    ch = MagicMock()
    method = MagicMock()
    props = MagicMock()
    props.reply_to = "42"
    props.correlation_id = "corr-3"

    with patch.object(mockMarket.pool, "connection", return_value=mock_conn_cm):
        mockMarket.on_order(ch, method, props, b"fake-order-bytes")

    assert mock_response.amountFilled == 5
    assert mock_response.orderID == 99
    assert mock_response.successful is True
    # Two publishes: one fill notification + one order response
    assert ch.basic_publish.call_count == 2
    ch.basic_ack.assert_called_once()
    # DELETE for fully consumed sell + INSERT for resting buy
    execute_calls = [str(c) for c in mock_cur.execute.call_args_list]
    assert any("DELETE" in c for c in execute_calls)
    assert any("INSERT" in c for c in execute_calls)
