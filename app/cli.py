"""Tenant administration.

    python -m app.cli create-tenant acme
    python -m app.cli list-tenants

argparse rather than a CLI framework: three commands do not justify a
dependency.
"""

import argparse
import sys

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db import session_scope
from app.models import Tenant
from app.security import generate_api_key, generate_webhook_secret, hash_api_key


def create_tenant(name: str, rate_limit_per_minute: int | None = None) -> int:
    api_key = generate_api_key()
    webhook_secret = generate_webhook_secret()
    tenant = Tenant(
        name=name,
        api_key_hash=hash_api_key(api_key),
        webhook_secret=webhook_secret,
        rate_limit_per_minute=rate_limit_per_minute,
    )
    try:
        with session_scope() as session:
            session.add(tenant)
    except IntegrityError:
        print(f"tenant {name!r} already exists", file=sys.stderr)
        return 1

    # Printed once: only the hash was stored, so this is the only copy.
    print(f"tenant      {name}")
    print(f"id          {tenant.id}")
    print(f"api key     {api_key}")
    print(f"webhook secret  {webhook_secret}")
    print()
    print("curl -sS -X POST http://localhost:8000/v1/notifications \\")
    print(f"  -H 'Authorization: Bearer {api_key}' \\")
    print("  -H 'Content-Type: application/json' \\")
    print("  -d '{\"channel\":\"in_app\",\"recipient\":\"user-42\",\"body\":\"hello\"}'")
    return 0


def list_tenants() -> int:
    with session_scope() as session:
        rows = session.scalars(select(Tenant).order_by(Tenant.created_at)).all()
    if not rows:
        print("no tenants yet - run: python -m app.cli create-tenant <name>")
        return 0
    for row in rows:
        override = row.rate_limit_per_minute if row.rate_limit_per_minute is not None else "default"
        print(f"{row.id}  {row.name:<24} rate_limit={override}  created={row.created_at:%Y-%m-%d %H:%M}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-tenant", help="create a tenant and print its API key")
    create.add_argument("name")
    create.add_argument("--rate-limit-per-minute", type=int, default=None)

    sub.add_parser("list-tenants", help="list tenants")

    args = parser.parse_args(argv)
    if args.command == "create-tenant":
        return create_tenant(args.name, args.rate_limit_per_minute)
    if args.command == "list-tenants":
        return list_tenants()
    parser.error(f"unknown command {args.command!r}")  # pragma: no cover
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
