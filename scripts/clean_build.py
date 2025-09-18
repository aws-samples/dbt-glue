#!/usr/bin/env python3
"""Clean build script to prevent duplicate macro issues."""

import shutil
import subprocess
import sys
from pathlib import Path


def main():
    """Clean build artifacts and rebuild package."""
    project_root = Path(__file__).parent.parent
    
    # Clean build artifacts
    for dir_name in ["build", "dist", "*.egg-info"]:
        for path in project_root.glob(dir_name):
            if path.exists():
                print(f"Removing {path}")
                shutil.rmtree(path)
    
    # Run the test to ensure no duplicates
    print("Running duplicate macro test...")
    result = subprocess.run([sys.executable, "-m", "pytest", "tests/unit/test_no_duplicate_macros.py", "-v"], 
                          cwd=project_root)
    if result.returncode != 0:
        print("Duplicate macro test failed!")
        return 1
    
    # Build the package
    print("Building package...")
    subprocess.run([sys.executable, "setup.py", "clean", "--all"], cwd=project_root)
    subprocess.run([sys.executable, "setup.py", "sdist", "bdist_wheel"], cwd=project_root, check=True)
    
    print("Clean build completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
