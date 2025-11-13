#!/usr/bin/env python3
"""
Development scripts for the datacollective package.
Run with: python scripts/dev.py <command>
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str]) -> int:
    """Run a command and return its exit code."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def format_code() -> int:
    """Format code with Black."""
    print("🎨 Formatting code with Black...")
    return run_command(["uv", "run", "black", "src/", "tests/"])


def format_check() -> int:
    """Verify code formatting with Black without modifying files."""
    print("🎨 Checking formatting with Black...")
    return run_command(["uv", "run", "black", "--check", "src/", "tests/"])


def lint_code() -> int:
    """Lint code with Ruff."""
    print("🔍 Linting code with Ruff...")
    return run_command(["uv", "run", "ruff", "check", "src/", "tests/"])


def type_check() -> int:
    """Type check with MyPy."""
    print("🔬 Type checking with MyPy...")
    return run_command(["uv", "run", "mypy", "src/"])


def fix_lint() -> int:
    """Fix linting issues automatically."""
    print("🔧 Fixing linting issues...")
    return run_command(["uv", "run", "ruff", "check", "--fix", "src/", "tests/"])


def run_tests() -> int:
    """Run tests with pytest."""
    print("🧪 Running tests...")
    return run_command(["uv", "run", "pytest", "tests/"])


def bump_version(part: str) -> int:
    """Bump version using bump2version."""
    print(f"📦 Bumping {part} version...")
    return run_command(["uv", "run", "bump2version", part])


def show_version() -> int:
    """Show current version."""
    print("📋 Current version information:")

    # Read version from pyproject.toml
    pyproject_path = Path("pyproject.toml")
    if pyproject_path.exists():
        content = pyproject_path.read_text()
        for line in content.split("\n"):
            if line.strip().startswith("version = "):
                version = line.split('"')[1]
                print(f"  pyproject.toml: {version}")
                break

    # Read version from __init__.py
    init_path = Path("src/datacollective/__init__.py")
    if init_path.exists():
        content = init_path.read_text()
        for line in content.split("\n"):
            if "__version__" in line:
                version = line.split('"')[1]
                print(f"  __init__.py: {version}")
                break

    return 0


def clean_build() -> int:
    """Clean build artifacts."""
    print("🧹 Cleaning build artifacts...")
    import shutil
    import os

    # Remove dist directory
    if os.path.exists("dist"):
        shutil.rmtree("dist")
        print("  Removed dist/ directory")

    # Remove build directory
    if os.path.exists("build"):
        shutil.rmtree("build")
        print("  Removed build/ directory")

    # Remove __pycache__ directories
    for root, dirs, files in os.walk("."):
        for dir_name in dirs[:]:  # Use slice to avoid modifying list while iterating
            if dir_name == "__pycache__":
                shutil.rmtree(os.path.join(root, dir_name))
                print(f"  Removed {os.path.join(root, dir_name)}")
                dirs.remove(dir_name)

    print("✅ Cleanup complete!")
    return 0


def build_package() -> int:
    """Build the package."""
    print("📦 Building package...")
    return run_command(["uv", "build"])


def publish_package(index: str = "pypi") -> int:
    """Publish package to PyPI or TestPyPI."""
    print(f"🚀 Publishing to {index}...")

    # Clean first
    if clean_build() != 0:
        print("❌ Clean failed")
        return 1

    # Build package
    if build_package() != 0:
        print("❌ Build failed")
        return 1

    # Publish
    if index == "testpypi":
        return run_command(["uv", "publish", "--index", "testpypi"])
    else:
        return run_command(["uv", "publish"])


def publish_with_bump(index: str = "pypi", part: str = "patch") -> int:
    """Bump version and publish package to PyPI or TestPyPI."""
    print(f"🚀 Bumping {part} version and publishing to {index}...")

    # Show current version
    print("📋 Current version:")
    if show_version() != 0:
        print("❌ Failed to get current version")
        return 1

    # Run all checks before bumping so we don't consume versions on failure
    print("🔍 Running pre-publish checks...")
    if all_checks() != 0:
        print("❌ Pre-publish checks failed")
        return 1

    # Bump version
    if bump_version(part) != 0:
        print("❌ Version bump failed")
        return 1

    # Show new version
    print("📋 New version:")
    if show_version() != 0:
        print("❌ Failed to get new version")
        return 1

    # Publish
    return publish_package(index)


def all_checks() -> int:
    """Run all checks: format, lint, type check, and tests."""
    print("🚀 Running all checks...")

    # Check formatting
    if format_check() != 0:
        print("❌ Formatting check failed")
        return 1

    # Run linting
    if lint_code() != 0:
        print("❌ Linting failed")
        return 1

    # Type check
    if type_check() != 0:
        print("❌ Type checking failed")
        return 1

    # Run tests
    if run_tests() != 0:
        print("❌ Tests failed")
        return 1

    print("✅ All checks passed!")
    return 0


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/dev.py <command>")
        print("Commands:")
        print("  format     - Format code with Black")
        print("  lint       - Lint code with Ruff")
        print("  typecheck  - Type check with MyPy")
        print("  fix        - Fix linting issues automatically")
        print("  test       - Run tests")
        print("  all        - Run all checks")
        print("  clean      - Clean build artifacts")
        print("  build      - Build package")
        print("  publish    - Clean, build, and publish to PyPI")
        print("  publish-test - Clean, build, and publish to TestPyPI")
        print("  publish-bump - Bump patch version and publish to PyPI")
        print("  publish-bump-test - Bump patch version and publish to TestPyPI")
        print("  version    - Show current version")
        print("  bump-patch - Bump patch version (0.0.1 -> 0.0.2)")
        print("  bump-minor - Bump minor version (0.0.1 -> 0.1.0)")
        print("  bump-major - Bump major version (0.0.1 -> 1.0.0)")
        return 1

    command = sys.argv[1].lower()

    # Handle version bumping commands
    if command.startswith("bump-"):
        part = command.split("-")[1]
        if part in ["patch", "minor", "major"]:
            return bump_version(part)
        else:
            print(f"Unknown version part: {part}")
            return 1

    # Handle publish commands
    if command == "publish":
        return publish_package("pypi")
    elif command == "publish-test":
        return publish_package("testpypi")
    elif command == "publish-bump":
        return publish_with_bump("pypi", "patch")
    elif command == "publish-bump-test":
        return publish_with_bump("testpypi", "patch")

    commands = {
        "format": format_code,
        "lint": lint_code,
        "typecheck": type_check,
        "fix": fix_lint,
        "test": run_tests,
        "all": all_checks,
        "clean": clean_build,
        "build": build_package,
        "version": show_version,
    }

    if command not in commands:
        print(f"Unknown command: {command}")
        return 1

    return commands[command]()


if __name__ == "__main__":
    sys.exit(main())
