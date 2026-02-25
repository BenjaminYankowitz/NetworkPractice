"""
Stub out modules that establish network connections at import time so that
mockMarket.py can be imported in a test environment without running services.
"""
import sys
import os
from unittest.mock import MagicMock

# Ensure DATABASE_URL is defined before load_dotenv() runs.
os.environ.setdefault("EXCHANGE_DATABASE_URL", "postgresql://test:test@localhost/testdb")

# Stub psycopg_pool so ConnectionPool() does not connect to PostgreSQL.
_fake_pool = MagicMock()
sys.modules["psycopg_pool"] = MagicMock(
    ConnectionPool=MagicMock(return_value=_fake_pool),
    PoolTimeout=Exception,
)

# Stub pika so BlockingConnection() does not connect to RabbitMQ.
sys.modules["pika"] = MagicMock()

# Stub marketMessages_pb2 (generated protobuf — may not be compiled).
sys.modules["marketMessages_pb2"] = MagicMock()

# Add market/ parent directory so `import mockMarket` resolves correctly
# when pytest is run from market/ or from the repo root.
_market_dir = os.path.join(os.path.dirname(__file__), "..")
if _market_dir not in sys.path:
    sys.path.insert(0, _market_dir)
