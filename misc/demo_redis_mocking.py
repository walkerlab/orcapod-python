#!/usr/bin/env python3
"""
Demonstration script showing that RedisCacher tests work without a real Redis server.

This script shows how the mock Redis setup allows testing of Redis functionality
without requiring an actual Redis installation or server.
"""

from unittest.mock import patch


# Mock Redis exceptions
class MockRedisError(Exception):
    """Mock for redis.RedisError"""

    pass


class MockConnectionError(Exception):
    """Mock for redis.ConnectionError"""

    pass


class MockRedis:
    """Mock Redis client for testing."""

    def __init__(self, fail_connection=False, fail_operations=False):
        self.data = {}
        self.fail_connection = fail_connection
        self.fail_operations = fail_operations
        self.ping_called = False

    def ping(self):
        self.ping_called = True
        if self.fail_connection:
            raise MockConnectionError("Connection failed")
        return True

    def set(self, key, value, ex=None):
        if self.fail_operations:
            raise MockRedisError("Operation failed")
        self.data[key] = value
        return True

    def get(self, key):
        if self.fail_operations:
            raise MockRedisError("Operation failed")
        return self.data.get(key)

    def delete(self, *keys):
        if self.fail_operations:
            raise MockRedisError("Operation failed")
        deleted = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                deleted += 1
        return deleted

    def keys(self, pattern):
        if self.fail_operations:
            raise MockRedisError("Operation failed")
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return [key for key in self.data.keys() if key.startswith(prefix)]
        return [key for key in self.data.keys() if key == pattern]


def demonstrate_redis_mocking():
    """Demonstrate that RedisCacher works with mocked Redis."""

    # Patch the Redis availability and exceptions
    with (
        patch("orcapod.hashing.string_cachers.REDIS_AVAILABLE", True),
        patch("orcapod.hashing.string_cachers.redis.RedisError", MockRedisError),
        patch(
            "orcapod.hashing.string_cachers.redis.ConnectionError",
            MockConnectionError,
        ),
    ):
        from orcapod.hashing.string_cachers import RedisCacher

        # Create a mock Redis instance
        mock_redis = MockRedis()

        print("🎭 Creating RedisCacher with mocked Redis...")
        cacher = RedisCacher(connection=mock_redis, key_prefix="demo:")

        print("✅ RedisCacher created successfully (no real Redis server needed!)")
        print(f"🔗 Connection status: {cacher.is_connected()}")

        # Test basic operations
        print("\n📝 Testing basic operations...")
        cacher.set_cached("test_key", "test_value")
        result = cacher.get_cached("test_key")
        print(f"   Set and retrieved: test_key -> {result}")

        # Show the mock Redis data
        print(f"   Mock Redis data: {dict(mock_redis.data)}")

        # Test failure simulation
        print("\n💥 Testing failure simulation...")
        mock_redis.fail_operations = True
        result = cacher.get_cached("test_key")
        print(f"   After simulated failure: {result}")
        print(f"🔗 Connection status after failure: {cacher.is_connected()}")

        # Test recovery
        print("\n🔄 Testing connection recovery...")
        mock_redis.fail_operations = False
        success = cacher.reset_connection()
        print(f"   Reset successful: {success}")
        print(f"🔗 Connection status after reset: {cacher.is_connected()}")

        print(
            "\n🎉 All operations completed successfully without requiring a Redis server!"
        )


if __name__ == "__main__":
    demonstrate_redis_mocking()
