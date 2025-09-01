# safedirstore.py - SafeDirDataStore implementation

import errno
import fcntl
import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union

from ..file_utils import atomic_copy, atomic_write

logger = logging.getLogger(__name__)


class FileLockError(Exception):
    """Exception raised when a file lock cannot be acquired"""

    pass


@contextmanager
def file_lock(
    lock_path: str | Path,
    shared: bool = False,
    timeout: float = 30.0,
    delay: float = 0.1,
    stale_threshold: float = 3600.0,
):
    """
    A context manager for file locking that supports both shared and exclusive locks.

    Args:
        lock_path: Path to the lock file
        shared: If True, acquire a shared (read) lock; if False, acquire an exclusive (write) lock
        timeout: Maximum time to wait for the lock in seconds
        delay: Time between retries in seconds
        stale_threshold: Time in seconds after which a lock is considered stale

    Yields:
        None when the lock is acquired

    Raises:
        FileLockError: If the lock cannot be acquired within the timeout
    """
    lock_path = Path(lock_path)
    lock_file = f"{lock_path}.lock"

    # Ensure parent directory exists
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    # Choose lock type based on shared flag
    lock_type = fcntl.LOCK_SH if shared else fcntl.LOCK_EX

    # Add non-blocking flag for the initial attempt
    lock_type_nb = lock_type | fcntl.LOCK_NB

    fd = None
    start_time = time.time()

    try:
        while True:
            try:
                # Open the lock file (create if it doesn't exist)
                fd = os.open(lock_file, os.O_CREAT | os.O_RDWR)

                try:
                    # Try to acquire the lock in non-blocking mode
                    fcntl.flock(fd, lock_type_nb)

                    # If we get here, lock was acquired
                    if not shared:  # For exclusive locks only
                        # Write PID and timestamp to lock file
                        os.ftruncate(fd, 0)  # Clear the file
                        os.write(fd, f"{os.getpid()},{time.time()}".encode())

                    break  # Exit the retry loop - we got the lock

                except IOError as e:
                    # Close the file descriptor if we couldn't acquire the lock
                    if fd is not None:
                        os.close(fd)
                        fd = None

                    if e.errno != errno.EAGAIN:
                        # If it's not "resource temporarily unavailable", re-raise
                        raise

                    # Check if the lock file is stale (only for exclusive locks)
                    if os.path.exists(lock_file) and not shared:
                        try:
                            with open(lock_file, "r") as f:
                                content = f.read().strip()
                                if "," in content:
                                    pid_str, timestamp_str = content.split(",", 1)
                                    lock_pid = int(pid_str)
                                    lock_time = float(timestamp_str)

                                    # Check if process exists
                                    process_exists = True
                                    try:
                                        os.kill(lock_pid, 0)
                                    except OSError:
                                        process_exists = False

                                    # Check if lock is stale
                                    if (
                                        not process_exists
                                        or time.time() - lock_time > stale_threshold
                                    ):
                                        logger.warning(
                                            f"Removing stale lock: {lock_file}"
                                        )
                                        os.unlink(lock_file)
                                        continue  # Try again immediately
                        except (ValueError, IOError):
                            # If we can't read the lock file properly, continue with retry
                            pass
            except Exception as e:
                logger.debug(
                    f"Error while trying to acquire lock {lock_file}: {str(e)}"
                )

                # If fd was opened, make sure it's closed
                if fd is not None:
                    os.close(fd)
                    fd = None

            # Check if we've exceeded the timeout
            if time.time() - start_time >= timeout:
                if fd is not None:
                    os.close(fd)
                lock_type_name = "shared" if shared else "exclusive"
                raise FileLockError(
                    f"Couldn't acquire {lock_type_name} lock on {lock_file} "
                    f"after {timeout} seconds"
                )

            # Sleep before retrying
            time.sleep(delay)

        # If we get here, we've acquired the lock
        logger.debug(
            f"Acquired {'shared' if shared else 'exclusive'} lock on {lock_file}"
        )

        # Yield control back to the caller
        yield

    finally:
        # Release the lock and close the file descriptor
        if fd is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

            # Remove the lock file only if it was an exclusive lock
            if not shared:
                try:
                    os.unlink(lock_file)
                except OSError as e:
                    logger.warning(f"Failed to remove lock file {lock_file}: {str(e)}")

            logger.debug(
                f"Released {'shared' if shared else 'exclusive'} lock on {lock_file}"
            )


class SafeDirDataStore:
    """
    A thread-safe and process-safe directory-based data store for memoization.
    Uses file locks and atomic operations to ensure consistency.
    """

    def __init__(
        self,
        store_dir="./pod_data",
        copy_files=True,
        preserve_filename=True,
        overwrite=False,
        lock_timeout=30,
        lock_stale_threshold=3600,
    ):
        """
        Initialize the data store.

        Args:
            store_dir: Base directory for storing data
            copy_files: Whether to copy files to the data store
            preserve_filename: Whether to preserve original filenames
            overwrite: Whether to overwrite existing entries
            lock_timeout: Timeout for acquiring locks in seconds
            lock_stale_threshold: Time in seconds after which a lock is considered stale
        """
        self.store_dir = Path(store_dir)
        self.copy_files = copy_files
        self.preserve_filename = preserve_filename
        self.overwrite = overwrite
        self.lock_timeout = lock_timeout
        self.lock_stale_threshold = lock_stale_threshold

        # Create the data directory if it doesn't exist
        self.store_dir.mkdir(parents=True, exist_ok=True)

    def _get_output_dir(self, function_name, content_hash, packet):
        """Get the output directory for a specific packet"""
        from orcapod.hashing.legacy_core import hash_dict

        packet_hash = hash_dict(packet)
        return self.store_dir / function_name / content_hash / str(packet_hash)

    def memoize(
        self,
        function_name: str,
        content_hash: str,
        packet: dict,
        output_packet: dict,
    ) -> dict:
        """
        Memoize the output packet for a given store, content hash, and input packet.
        Uses file locking to ensure thread safety and process safety.

        Args:
            function_name: Name of the function
            content_hash: Hash of the function/operation
            packet: Input packet
            output_packet: Output packet to memoize

        Returns:
            The memoized output packet with paths adjusted to the store

        Raises:
            FileLockError: If the lock cannot be acquired
            ValueError: If the entry already exists and overwrite is False
        """
        output_dir = self._get_output_dir(function_name, content_hash, packet)
        info_path = output_dir / "_info.json"
        lock_path = output_dir / "_lock"
        completion_marker = output_dir / "_complete"

        # Create the output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # First check if we already have a completed entry (with a shared lock)
        try:
            with file_lock(lock_path, shared=True, timeout=self.lock_timeout):
                if completion_marker.exists() and not self.overwrite:
                    logger.info(f"Entry already exists for packet {packet}")
                    return self.retrieve_memoized(function_name, content_hash, packet)
        except FileLockError:
            logger.warning("Could not acquire shared lock to check completion status")
            # Continue to try with exclusive lock

        # Now try to acquire an exclusive lock for writing
        with file_lock(
            lock_path,
            shared=False,
            timeout=self.lock_timeout,
            stale_threshold=self.lock_stale_threshold,
        ):
            # Double-check if the entry already exists (another process might have created it)
            if completion_marker.exists() and not self.overwrite:
                logger.info(
                    f"Entry already exists for packet {packet} (verified with exclusive lock)"
                )
                return self.retrieve_memoized(function_name, content_hash, packet)

            # Check for partial results and clean up if necessary
            partial_marker = output_dir / "_partial"
            if partial_marker.exists():
                partial_time = float(partial_marker.read_text().strip())
                if time.time() - partial_time > self.lock_stale_threshold:
                    logger.warning(
                        f"Found stale partial results in {output_dir}, cleaning up"
                    )
                    for item in output_dir.glob("*"):
                        if item.name not in ("_lock", "_lock.lock"):
                            if item.is_file():
                                item.unlink(missing_ok=True)
                            else:
                                import shutil

                                shutil.rmtree(item, ignore_errors=True)

            # Create partial marker
            atomic_write(partial_marker, str(time.time()))

            try:
                # Process files
                new_output_packet = {}
                if self.copy_files:
                    for key, value in output_packet.items():
                        value_path = Path(value)

                        if self.preserve_filename:
                            relative_output_path = value_path.name
                        else:
                            # Preserve the suffix of the original if present
                            relative_output_path = key + value_path.suffix

                        output_path = output_dir / relative_output_path

                        # Use atomic copy to ensure consistency
                        atomic_copy(value_path, output_path)

                        # Register the key with the new path
                        new_output_packet[key] = str(relative_output_path)
                else:
                    new_output_packet = output_packet.copy()

                # Write info JSON atomically
                atomic_write(info_path, json.dumps(new_output_packet, indent=2))

                # Create completion marker (atomic write ensures it's either fully there or not at all)
                atomic_write(completion_marker, str(time.time()))

                logger.info(f"Stored output for packet {packet} at {output_dir}")

                # Retrieve the memoized packet to ensure consistency
                # We don't need to acquire a new lock since we already have an exclusive lock
                return self._retrieve_without_lock(
                    function_name, content_hash, packet, output_dir
                )

            finally:
                # Remove partial marker if it exists
                if partial_marker.exists():
                    partial_marker.unlink(missing_ok=True)

    def retrieve_memoized(
        self, function_name: str, content_hash: str, packet: dict
    ) -> Optional[dict]:
        """
        Retrieve a memoized output packet.

        Uses a shared lock to allow concurrent reads while preventing writes during reads.

        Args:
            function_name: Name of the function
            content_hash: Hash of the function/operation
            packet: Input packet

        Returns:
            The memoized output packet with paths adjusted to absolute paths,
            or None if the packet is not found
        """
        output_dir = self._get_output_dir(function_name, content_hash, packet)
        lock_path = output_dir / "_lock"

        # Use a shared lock for reading to allow concurrent reads
        try:
            with file_lock(lock_path, shared=True, timeout=self.lock_timeout):
                return self._retrieve_without_lock(
                    function_name, content_hash, packet, output_dir
                )
        except FileLockError:
            logger.warning(f"Could not acquire shared lock to read {output_dir}")
            return None

    def _retrieve_without_lock(
        self, function_name: str, content_hash: str, packet: dict, output_dir: Path
    ) -> Optional[dict]:
        """
        Helper to retrieve a memoized packet without acquiring a lock.

        This is used internally when we already have a lock.

        Args:
            function_name: Name of the function
            content_hash: Hash of the function/operation
            packet: Input packet
            output_dir: Directory containing the output

        Returns:
            The memoized output packet with paths adjusted to absolute paths,
            or None if the packet is not found
        """
        info_path = output_dir / "_info.json"
        completion_marker = output_dir / "_complete"

        # Only return if the completion marker exists
        if not completion_marker.exists():
            logger.info(f"No completed output found for packet {packet}")
            return None

        if not info_path.exists():
            logger.warning(
                f"Completion marker exists but info file missing for {packet}"
            )
            return None

        try:
            with open(info_path, "r") as f:
                output_packet = json.load(f)

            # Update paths to be absolute
            for key, value in output_packet.items():
                file_path = output_dir / value
                if not file_path.exists():
                    logger.warning(f"Referenced file {file_path} does not exist")
                    return None
                output_packet[key] = str(file_path)

            logger.info(f"Retrieved output for packet {packet} from {info_path}")
            return output_packet

        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {info_path}")
            return None
        except Exception as e:
            logger.error(f"Error loading memoized output for packet {packet}: {e}")
            return None

    def clear_store(self, function_name: str) -> None:
        """
        Clear a specific store.

        Args:
            function_name: Name of the function to clear
        """
        import shutil

        store_path = self.store_dir / function_name
        if store_path.exists():
            shutil.rmtree(store_path)

    def clear_all_stores(self) -> None:
        """Clear all stores"""
        import shutil

        if self.store_dir.exists():
            shutil.rmtree(self.store_dir)
            self.store_dir.mkdir(parents=True, exist_ok=True)

    def clean_stale_data(self, function_name=None, max_age=86400):
        """
        Clean up stale data in the store.

        Args:
            function_name: Optional name of the function to clean, or None for all functions
            max_age: Maximum age of data in seconds before it's considered stale
        """
        import shutil

        if function_name is None:
            # Clean all stores
            for store_dir in self.store_dir.iterdir():
                if store_dir.is_dir():
                    self.clean_stale_data(store_dir.name, max_age)
            return

        store_path = self.store_dir / function_name
        if not store_path.is_dir():
            return

        now = time.time()

        # Find all directories with partial markers
        for content_hash_dir in store_path.iterdir():
            if not content_hash_dir.is_dir():
                continue

            for packet_hash_dir in content_hash_dir.iterdir():
                if not packet_hash_dir.is_dir():
                    continue

                # Try to acquire an exclusive lock with a short timeout
                lock_path = packet_hash_dir / "_lock"
                try:
                    with file_lock(lock_path, shared=False, timeout=1.0):
                        partial_marker = packet_hash_dir / "_partial"
                        completion_marker = packet_hash_dir / "_complete"

                        # Check for partial results with no completion marker
                        if partial_marker.exists() and not completion_marker.exists():
                            try:
                                partial_time = float(partial_marker.read_text().strip())
                                if now - partial_time > max_age:
                                    logger.info(
                                        f"Cleaning up stale data in {packet_hash_dir}"
                                    )
                                    shutil.rmtree(packet_hash_dir)
                            except (ValueError, IOError):
                                # If we can't read the marker, assume it's stale
                                logger.info(
                                    f"Cleaning up invalid partial data in {packet_hash_dir}"
                                )
                                shutil.rmtree(packet_hash_dir)
                except FileLockError:
                    # Skip if we couldn't acquire the lock
                    continue
