#!/usr/bin/env python3
"""Render least-privilege Neo4j Enterprise reader grants for one ACL scope.

The output is intended for an authorized administrator to review and pipe to
cypher-shell. Values are deliberately restricted instead of escaped loosely.
"""

from __future__ import annotations

import argparse
import hashlib
import re

SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")


def checked(value: str) -> str:
    if not SAFE.fullmatch(value):
        raise ValueError("security labels must match the restricted identifier alphabet")
    return value


def quote(value: str) -> str:
    return "'" + checked(value).replace("'", "''") + "'"


def render(tenant: str, repos: list[str], groups: list[str], database: str) -> str:
    tenant = checked(tenant)
    database = checked(database)
    if not repos or not groups:
        raise ValueError("repository and ACL scopes must be non-empty")
    repos = sorted(set(map(checked, repos)))
    groups = sorted(set(map(checked, groups)))
    digest = hashlib.sha256(
        "\0".join([tenant, *repos, *groups]).encode("utf-8")
    ).hexdigest()[:16]
    role = f"eci_reader_{digest}"
    repo_values = ", ".join(map(quote, repos))
    group_values = ", ".join(map(quote, groups))
    return f"""CREATE ROLE `{role}` IF NOT EXISTS;
GRANT ACCESS ON DATABASE `{database}` TO `{role}`;
GRANT MATCH {{ * }} ON GRAPH `{database}`
  FOR (n:CodeNode) WHERE n.tenant_id = {quote(tenant)}
    AND n.repo IN [{repo_values}]
    AND n.acl_group IN [{group_values}]
  TO `{role}`;
GRANT TRAVERSE ON GRAPH `{database}` RELATIONSHIPS * TO `{role}`;
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--repo", action="append", required=True)
    parser.add_argument("--acl-group", action="append", required=True)
    parser.add_argument("--database", default="neo4j")
    args = parser.parse_args()
    print(render(args.tenant, args.repo, args.acl_group, args.database), end="")


if __name__ == "__main__":
    main()
