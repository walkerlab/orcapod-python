# file_ops.py - Atomic file operations module

import os
import time
import logging
from pathlib import Path
from typing import Union, Optional, Tuple

logger = logging.getLogger(__name__)

def atomic_write(file_path: Union[str, Path], content: str) -> Path:
    """
    Atomically write content to a file.
    
    This function writes content to a temporary file and then atomically
    renames it to the target file path, ensuring that other processes never
    see a partially-written file.
    
    Args:
        file_path: Target file path
        content: Content to write
        
    Returns:
        Path object to the written file
        
    Raises:
        OSError: If the file cannot be written
    """
    file_path = Path(file_path)
    temp_path = file_path.with_name(f"{file_path.name}.tmp{os.getpid()}")
    
    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Write content to a temporary file
        with open(temp_path, 'w') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())  # Force flush to disk
        
        # Atomic rename
        os.rename(temp_path, file_path)
        return file_path
    except Exception as e:
        logger.error(f"Error writing file {file_path}: {str(e)}")
        raise
    finally:
        # Clean up the temporary file if it exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def atomic_write_bytes(file_path: Union[str, Path], content: bytes) -> Path:
    """
    Atomically write binary content to a file.
    
    This function writes binary content to a temporary file and then atomically
    renames it to the target file path.
    
    Args:
        file_path: Target file path
        content: Binary content to write
        
    Returns:
        Path object to the written file
        
    Raises:
        OSError: If the file cannot be written
    """
    file_path = Path(file_path)
    temp_path = file_path.with_name(f"{file_path.name}.tmp{os.getpid()}")
    
    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Write content to a temporary file
        with open(temp_path, 'wb') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())  # Force flush to disk
        
        # Atomic rename
        os.rename(temp_path, file_path)
        return file_path
    except Exception as e:
        logger.error(f"Error writing file {file_path}: {str(e)}")
        raise
    finally:
        # Clean up the temporary file if it exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def atomic_copy(source_path: Union[str, Path], dest_path: Union[str, Path]) -> Path:
    """
    Atomically copy a file.
    
    This function copies a file to a temporary location and then atomically
    renames it to the target path, ensuring that other processes never
    see a partially-copied file.
    
    Args:
        source_path: Source file path
        dest_path: Destination file path
        
    Returns:
        Path object to the copied file
        
    Raises:
        OSError: If the file cannot be copied
        FileNotFoundError: If the source file does not exist
    """
    import shutil
    
    source_path = Path(source_path)
    dest_path = Path(dest_path)
    temp_path = dest_path.with_name(f"{dest_path.name}.tmp{os.getpid()}")
    
    # Check if source exists
    if not source_path.exists():
        raise FileNotFoundError(f"Source file does not exist: {source_path}")
    
    # Ensure parent directory exists
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Copy to temporary file
        shutil.copy2(source_path, temp_path)
        
        # Ensure the data is written to disk
        with open(temp_path, 'a') as f:
            os.fsync(f.fileno())
        
        # Atomic rename
        os.rename(temp_path, dest_path)
        return dest_path
    except Exception as e:
        logger.error(f"Error copying file from {source_path} to {dest_path}: {str(e)}")
        raise
    finally:
        # Clean up the temporary file if it exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def atomic_append(file_path: Union[str, Path], content: str) -> Path:
    """
    Atomically append content to a file.
    
    This function reads the existing content, appends the new content,
    and then atomically writes the result back to the file.
    
    Args:
        file_path: Target file path
        content: Content to append
        
    Returns:
        Path object to the appended file
        
    Raises:
        OSError: If the file cannot be written
    """
    file_path = Path(file_path)
    
    # Read existing content if file exists
    existing_content = ""
    if file_path.exists():
        try:
            with open(file_path, 'r') as f:
                existing_content = f.read()
        except Exception as e:
            logger.error(f"Error reading file {file_path} for append: {str(e)}")
            raise
    
    # Write the combined content atomically
    return atomic_write(file_path, existing_content + content)


def atomic_replace(
    file_path: Union[str, Path], 
    pattern: str, 
    replacement: str,
    count: int = -1
) -> Tuple[Path, int]:
    """
    Atomically replace text in a file.
    
    This function reads the existing content, performs the replacement,
    and then atomically writes the result back to the file.
    
    Args:
        file_path: Target file path
        pattern: Pattern to replace
        replacement: Replacement text
        count: Maximum number of replacements (default: unlimited)
        
    Returns:
        Tuple of (Path object to the file, number of replacements made)
        
    Raises:
        OSError: If the file cannot be read or written
        FileNotFoundError: If the file does not exist
    """
    file_path = Path(file_path)
    
    # Check if file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")
    
    # Read existing content
    try:
        with open(file_path, 'r') as f:
            existing_content = f.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path} for replacement: {str(e)}")
        raise
    
    # Perform replacement
    new_content, num_replacements = existing_content, 0
    if count == -1:
        # Replace all occurrences
        new_content = existing_content.replace(pattern, replacement)
        num_replacements = existing_content.count(pattern)
    else:
        # Replace only up to count occurrences
        new_content = ""
        remaining = existing_content
        for _ in range(count):
            if pattern not in remaining:
                break
            pos = remaining.find(pattern)
            new_content += remaining[:pos] + replacement
            remaining = remaining[pos + len(pattern):]
            num_replacements += 1
        new_content += remaining
    
    # Write the new content atomically
    return atomic_write(file_path, new_content), num_replacements


def is_file_locked(file_path: Union[str, Path]) -> bool:
    """
    Check if a file is locked.
    
    This function attempts to open the file for writing in non-blocking mode
    and checks if it fails with a "resource temporarily unavailable" error.
    
    Args:
        file_path: File path to check
        
    Returns:
        True if the file is locked, False otherwise
    """
    import errno
    import fcntl
    
    file_path = Path(file_path)
    
    # If file doesn't exist, it's not locked
    if not file_path.exists():
        return False
    
    try:
        # Try to open the file and get an exclusive lock in non-blocking mode
        with open(file_path, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we get here, the file is not locked
            fcntl.flock(f, fcntl.LOCK_UN)
            return False
    except IOError as e:
        if e.errno == errno.EAGAIN:
            # Resource temporarily unavailable = the file is locked
            return True
        # Some other error - assume not locked
        return False
    except Exception:
        # Any other exception - assume not locked
        return False