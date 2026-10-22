"""Decrypt one ytclone data/attribute object using Python stdlib and OpenSSL.

This does not authenticate the archive manifest or verify plaintext checksums.
Use ytclone restore for the complete integrity checks.
"""

import argparse
import os
import subprocess
from pathlib import Path

PASSWORD_VARIABLE = b"YTCLONE_RECOVERY_PASSWORD"
ITERATIONS = 100_000


def read_password(path: Path) -> bytes:
    """Preserve the exact password bytes accepted by ytclone's file option."""
    password = path.read_bytes()
    if password.endswith(b"\r\n"):
        password = password[:-2]
    elif password.endswith(b"\n"):
        password = password[:-1]

    if len(password) < 32:
        raise ValueError("Encryption password must contain at least 32 bytes")
    if any(value in password for value in (b"\x00", b"\n", b"\r")):
        raise ValueError("Encryption password must not contain NUL or newlines")
    return password


def recover(*, executable: str, password: bytes, source: Path, target: Path) -> int:
    """Keep password bytes out of command arguments and the parent environment."""
    help_result = subprocess.run(
        [executable, "enc", "-help"], capture_output=True, check=False
    )
    options = help_result.stdout + help_result.stderr
    if b"-pbkdf2" not in options:
        raise ValueError("OpenSSL 1.1.1 or newer with PBKDF2 support is required")

    command = [
        executable,
        "enc",
        "-d",
        "-aes-256-cbc",
        "-pbkdf2",
        "-iter",
        str(ITERATIONS),
        "-md",
        "sha256",
        "-pass",
        f"env:{PASSWORD_VARIABLE.decode('ascii')}",
        "-in",
        str(source),
        "-out",
        str(target),
    ]
    if b"-saltlen" in options:
        command.extend(["-saltlen", "8"])

    environment = os.environb.copy()
    environment[PASSWORD_VARIABLE] = password
    return subprocess.run(command, env=environment, check=False).returncode


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--password-file", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--openssl", default="openssl", help="OpenSSL executable path")
    args = parser.parse_args()

    if not os.supports_bytes_environ:
        parser.exit(status=1, message="Recovery requires Linux or macOS.\n")
    try:
        password = read_password(args.password_file)
    except OSError:
        parser.exit(status=1, message="Cannot read encryption password file.\n")
    except ValueError as error:
        parser.exit(status=1, message=f"{error}\n")

    try:
        status = recover(
            executable=args.openssl,
            password=password,
            source=args.input,
            target=args.output,
        )
    except OSError:
        parser.exit(
            status=1,
            message=(
                "Cannot start OpenSSL; check its executable path and the "
                "operating-system argument/environment size limits.\n"
            ),
        )
    except ValueError as error:
        parser.exit(status=1, message=f"{error}\n")
    parser.exit(status=status if status >= 0 else 128 - status)


if __name__ == "__main__":
    main()
