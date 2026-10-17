"""Resumable copying between YTsaurus and S3 or local disk."""


def main() -> int:
    """Load and run the command-line entry point."""
    from .cli import main as cli_main

    return cli_main()
