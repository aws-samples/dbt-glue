#!/usr/bin/env python3
"""Clean build script with validation and flexible build options."""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def clean_artifacts(project_root):
    """Clean build artifacts."""
    for dir_name in ["build", "dist", "*.egg-info"]:
        for path in project_root.glob(dir_name):
            if path.exists():
                print(f"Removing {path}")
                shutil.rmtree(path)


def run_tests(project_root, skip_tests):
    """Run validation tests."""
    if skip_tests:
        print("Skipping tests...")
        return True
    
    print("Running duplicate macro test...")
    result = subprocess.run([sys.executable, "-m", "pytest", "tests/unit/test_no_duplicate_macros.py", "-v"], 
                          cwd=project_root)
    return result.returncode == 0


def build_package(project_root, build_type):
    """Build the package."""
    print(f"Building package ({build_type})...")
    
    # Ensure dist directory exists
    (project_root / "dist").mkdir(exist_ok=True)
    
    subprocess.run([sys.executable, "setup.py", "clean", "--all"], cwd=project_root)
    
    if build_type == "sdist":
        subprocess.run([sys.executable, "setup.py", "sdist"], cwd=project_root, check=True)
    elif build_type == "wheel":
        subprocess.run([sys.executable, "setup.py", "bdist_wheel"], cwd=project_root, check=True)
    else:  # both
        subprocess.run([sys.executable, "setup.py", "sdist", "bdist_wheel"], cwd=project_root, check=True)


def main():
    """Clean build artifacts and rebuild package."""
    parser = argparse.ArgumentParser(description="Clean build script with validation")
    parser.add_argument("--skip-tests", action="store_true", help="Skip validation tests")
    parser.add_argument("--build-type", choices=["sdist", "wheel", "both"], default="both",
                       help="Type of build to create")
    
    args = parser.parse_args()
    project_root = Path(__file__).parent.parent
    
    # Clean build artifacts
    clean_artifacts(project_root)
    
    # Run tests
    if not run_tests(project_root, args.skip_tests):
        print("Tests failed!")
        return 1
    
    # Build the package
    build_package(project_root, args.build_type)
    
    print("Clean build completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
