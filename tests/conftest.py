import pytest
import pytest_asyncio
import fakeredis.aioredis
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.database import Base, get_db
import app.queue as app_queue
import app.main as app_main
import app.worker as app_worker

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest_asyncio.fixture
async def test_redis():
    """In-memory Redis client instance overriding app modules."""
    fake_server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=fake_server, decode_responses=True)
    
    # Patch module-level Redis references
    app_queue.redis_client = client
    app_main.redis_client = client
    app_worker.redis_client = client

    yield client
    await client.flushall()
    await client.aclose()

@pytest_asyncio.fixture
async def test_db_session():
    """Isolated in-memory database engine and session."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Patch worker database session
    app_worker.AsyncSessionLocal = async_session

    async with async_session() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

@pytest_asyncio.fixture
async def client(test_db_session: AsyncSession, test_redis):
    """FastAPI test client with injected DB and Redis fixtures."""
    async def override_get_db():
        yield test_db_session

    app_main.app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app_main.app)

    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    app_main.app.dependency_overrides.clear()