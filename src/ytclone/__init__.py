"""Resumable YTsaurus / S3 copying."""


def main() -> int:
    """Load and run the command-line entry point."""
    from .cli import main as cli_main

    return cli_main()
