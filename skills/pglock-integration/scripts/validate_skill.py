#!/usr/bin/env python3
"""Validate the local Agent Skill layout without third-party dependencies."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


NAME_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def parse_frontmatter(skill_file: Path) -> tuple[dict[str, str], list[str]]:
    lines = skill_file.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("SKILL.md must start with YAML frontmatter ('---')")
    try:
        end = lines.index("---", 1)
    except ValueError as exc:
        raise ValueError("SKILL.md frontmatter is missing its closing '---'") from exc

    fields: dict[str, str] = {}
    for line_number, line in enumerate(lines[1:end], start=2):
        if not line.strip() or line.lstrip().startswith("#") or line[0].isspace():
            continue
        if ":" not in line:
            raise ValueError(f"frontmatter line {line_number} is not a key/value pair")
        key, value = line.split(":", 1)
        fields[key.strip()] = value.strip().strip('"\'')
    return fields, lines[end + 1 :]


def load_json(path: Path, label: str, errors: list[str]) -> object | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"cannot parse {label}: {exc}")
        return None


def validate_output_evals(skill_root: Path, skill_name: str, errors: list[str]) -> None:
    path = skill_root / "evals" / "evals.json"
    if not path.exists():
        errors.append("missing evals/evals.json")
        return
    document = load_json(path, "evals/evals.json", errors)
    if not isinstance(document, dict):
        errors.append("evals/evals.json must contain a JSON object")
        return
    if document.get("skill_name") != skill_name:
        errors.append("evals/evals.json skill_name must match SKILL.md name")
    cases = document.get("evals")
    if not isinstance(cases, list) or not cases:
        errors.append("evals/evals.json evals must be a non-empty array")
        return
    for index, case in enumerate(cases, start=1):
        if not isinstance(case, dict):
            errors.append(f"eval {index} must be an object")
            continue
        for field in ("id", "prompt", "expected_output", "assertions"):
            if field not in case:
                errors.append(f"eval {index} is missing {field}")
        if not isinstance(case.get("assertions"), list) or not case["assertions"]:
            errors.append(f"eval {index} assertions must be a non-empty array")


def validate_trigger_evals(skill_root: Path, skill_name: str, errors: list[str]) -> None:
    path = skill_root / "evals" / "trigger_queries.json"
    if not path.exists():
        errors.append("missing evals/trigger_queries.json")
        return
    document = load_json(path, "evals/trigger_queries.json", errors)
    if not isinstance(document, dict):
        errors.append("evals/trigger_queries.json must contain a JSON object")
        return
    if document.get("skill_name") != skill_name:
        errors.append("evals/trigger_queries.json skill_name must match SKILL.md name")
    for split in ("train", "validation"):
        cases = document.get(split)
        if not isinstance(cases, list) or not cases:
            errors.append(f"trigger query {split} must be a non-empty array")
            continue
        outcomes: set[bool] = set()
        for index, case in enumerate(cases, start=1):
            if not isinstance(case, dict):
                errors.append(f"trigger query {split} item {index} must be an object")
                continue
            if not isinstance(case.get("query"), str) or not case["query"].strip():
                errors.append(f"trigger query {split} item {index} needs a non-empty query")
            if not isinstance(case.get("should_trigger"), bool):
                errors.append(f"trigger query {split} item {index} needs a boolean should_trigger")
            else:
                outcomes.add(case["should_trigger"])
        if outcomes != {False, True}:
            errors.append(f"trigger query {split} must contain positive and negative cases")


def validate(skill_root: Path) -> dict[str, object]:
    errors: list[str] = []
    warnings: list[str] = []
    skill_file = skill_root / "SKILL.md"
    if not skill_file.is_file():
        errors.append("missing SKILL.md")
        return {"skill": skill_root.name, "valid": False, "errors": errors, "warnings": warnings}

    try:
        fields, body = parse_frontmatter(skill_file)
    except (OSError, ValueError) as exc:
        errors.append(str(exc))
        return {"skill": skill_root.name, "valid": False, "errors": errors, "warnings": warnings}

    name = fields.get("name", "")
    description = fields.get("description", "")
    if not name:
        errors.append("frontmatter name is required")
    elif len(name) > 64 or not NAME_RE.fullmatch(name):
        errors.append("frontmatter name must be 1-64 lowercase letters, numbers, and single hyphens")
    if name and skill_root.name != name:
        errors.append("frontmatter name must match the skill directory name")
    if not description:
        errors.append("frontmatter description is required")
    elif len(description) > 1024:
        errors.append("frontmatter description must be at most 1024 characters")
    compatibility = fields.get("compatibility", "")
    if len(compatibility) > 500:
        errors.append("frontmatter compatibility must be at most 500 characters")
    if not body or not any(line.strip() for line in body):
        errors.append("SKILL.md must contain instructions after frontmatter")
    if len(body) > 500:
        warnings.append(f"SKILL.md body has {len(body)} lines; keep it under 500 when possible")

    validate_output_evals(skill_root, name, errors)
    validate_trigger_evals(skill_root, name, errors)
    return {"skill": name or skill_root.name, "valid": not errors, "errors": errors, "warnings": warnings}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate an Agent Skill's metadata, layout, output evals, and trigger queries.",
        epilog="Example: python3 scripts/validate_skill.py --skill-root .",
    )
    parser.add_argument(
        "--skill-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="skill directory to validate (default: this script's skill directory)",
    )
    args = parser.parse_args()
    result = validate(args.skill_root.resolve())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    sys.exit(main())
