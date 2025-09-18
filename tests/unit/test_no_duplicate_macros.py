"""Test to ensure no duplicate macro definitions exist in the adapter.

This test is automatically run by scripts/build.py to prevent duplicate macro issues.
"""

import os
import re
from pathlib import Path
from collections import defaultdict


def test_no_duplicate_macros():
    """Test that there are no duplicate macro definitions in the dbt-glue adapter."""
    
    # Get the macros directory
    macros_dir = Path(__file__).parent.parent.parent / "dbt" / "include" / "glue" / "macros"
    
    # Dictionary to track macro names and their file locations
    macro_definitions = defaultdict(list)
    
    # Pattern to match macro definitions
    macro_pattern = re.compile(r'{%\s*materialization\s+(\w+)\s*,\s*adapter\s*=\s*[\'"]glue[\'"]')
    
    # Walk through all SQL files in the macros directory
    for sql_file in macros_dir.rglob("*.sql"):
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Find all macro definitions in this file
        matches = macro_pattern.findall(content)
        
        for macro_name in matches:
            # Store the relative path for cleaner error messages
            relative_path = sql_file.relative_to(macros_dir)
            macro_definitions[f"materialization_{macro_name}_glue"].append(str(relative_path))
    
    # Check for duplicates
    duplicates = {name: files for name, files in macro_definitions.items() if len(files) > 1}
    
    if duplicates:
        error_msg = "Found duplicate macro definitions:\n"
        for macro_name, files in duplicates.items():
            error_msg += f"  {macro_name}:\n"
            for file_path in files:
                error_msg += f"    - {file_path}\n"
        
        raise AssertionError(error_msg)


if __name__ == "__main__":
    test_no_duplicate_macros()
    print("✅ No duplicate macros found!")
