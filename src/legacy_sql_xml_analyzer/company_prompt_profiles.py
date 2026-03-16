from __future__ import annotations

import json
from typing import Any


COMPANY_PROMPT_PROFILES: dict[str, dict[str, Any]] = {
    "company-qwen3-classify": {
        "max_input_tokens": 12000,
        "max_output_tokens": 1200,
        "allowed_actions": [
            "classify the failure family",
            "list missing evidence",
            "return insufficient_evidence when needed",
        ],
        "forbidden_actions": [
            "do not invent SQL business semantics",
            "do not propose XML edits",
            "do not return prose outside JSON",
        ],
        "good_output_examples": [
            '{"cluster_id":"example","problem_type":"mapping_inference","missing_evidence":["external xml alias"],"confidence":"medium","insufficient_evidence":true}'
        ],
        "bad_output_examples": [
            "The issue is probably a typo. You should fix the XML and try again."
        ],
    },
    "company-qwen3-propose": {
        "max_input_tokens": 18000,
        "max_output_tokens": 1600,
        "allowed_actions": [
            "propose the smallest safe reusable rule",
            "propose insufficient_evidence when proof is missing",
        ],
        "forbidden_actions": [
            "do not invent new tables, columns, or business filters",
            "do not widen the blast radius without evidence",
            "do not return prose outside JSON",
        ],
        "good_output_examples": [
            '{"cluster_id":"example","problem_type":"mapping_inference","proposed_change_type":"profile_rule","proposed_rule_or_fix":{"rule_type":"external_xml_name_mapping"},"confidence":"medium","why":["repeated alias pattern"],"verification_steps":["simulate profile"],"risks":[],"insufficient_evidence":false}'
        ],
        "bad_output_examples": [
            "I think you should rewrite the SQL and merge two queries into one."
        ],
    },
    "company-qwen3-verify": {
        "max_input_tokens": 14000,
        "max_output_tokens": 1200,
        "allowed_actions": [
            "verify a prior proposal",
            "reject unsafe changes",
            "return needs_review or insufficient_evidence when needed",
        ],
        "forbidden_actions": [
            "do not invent a new rule in verify",
            "do not rewrite SQL business logic",
            "do not return prose outside JSON",
        ],
        "good_output_examples": [
            '{"cluster_id":"example","verdict":"needs_review","constraint_checks":["schema ok"],"blocking_risks":["missing evidence"],"recommended_next_step":"repair"}'
        ],
        "bad_output_examples": [
            "Looks good overall, but maybe add another rule."
        ],
    },
    "company-qwen3-java-phase": {
        "max_input_tokens": 22000,
        "max_output_tokens": 1800,
        "allowed_actions": [
            "complete only the requested Java BFF phase",
            "preserve Oracle 19c SQL semantics",
            "return JSON only",
        ],
        "forbidden_actions": [
            "do not merge multiple phases into one answer",
            "do not guess missing SQL logic",
            "do not return prose outside JSON",
        ],
        "good_output_examples": [
            '{"entry_query_id":"orders.xml:main:OrderSearch","service_logic":["validate request","call repository"],"controller_logic":["delegate to service"],"dto_contract_hints":["customerId required"],"error_handling":["translate sql exceptions"],"follow_up_questions":[]}'
        ],
        "bad_output_examples": [
            "Here is a full Spring Boot implementation with guessed SQL."
        ],
    },
}


def get_company_prompt_profile(name: str) -> dict[str, Any]:
    if name not in COMPANY_PROMPT_PROFILES:
        raise ValueError(f"Unknown company prompt profile: {name}")
    return COMPANY_PROMPT_PROFILES[name]


def build_response_template(schema: Any) -> Any:
    if isinstance(schema, dict):
        return {key: build_response_template(value) for key, value in schema.items()}
    if isinstance(schema, list):
        if not schema:
            return []
        return [build_response_template(schema[0])]
    return schema


def render_company_prompt(
    *,
    profile_name: str,
    title: str,
    objective_lines: list[str],
    evidence_sections: list[tuple[str, str]],
    schema: dict[str, Any],
    extra_constraints: list[str] | None = None,
) -> str:
    profile = get_company_prompt_profile(profile_name)
    lines = [
        title,
        "",
        "Goal:",
    ]
    lines.extend(f"- {line}" for line in objective_lines)
    lines.extend(
        [
            "",
            f"Profile: {profile_name}",
            f"Max input tokens: {profile['max_input_tokens']}",
            f"Max output tokens: {profile['max_output_tokens']}",
            "",
            "Allowed actions:",
        ]
    )
    lines.extend(f"- {line}" for line in profile["allowed_actions"])
    lines.extend(["", "Forbidden actions:"])
    lines.extend(f"- {line}" for line in profile["forbidden_actions"])
    if extra_constraints:
        lines.extend(["", "Additional constraints:"])
        lines.extend(f"- {line}" for line in extra_constraints)
    good_examples = profile.get("good_output_examples")
    if isinstance(good_examples, list) and good_examples:
        lines.extend(["", "Good output example:"])
        lines.extend(good_examples[:1])
    bad_examples = profile.get("bad_output_examples")
    if isinstance(bad_examples, list) and bad_examples:
        lines.extend(["", "Bad output example:"])
        lines.extend(bad_examples[:1])
    lines.extend(["", "Evidence:"])
    for header, body in evidence_sections:
        lines.extend([f"## {header}", body.rstrip(), ""])
    lines.extend(
        [
            "Return JSON only with this exact response template:",
            json.dumps(build_response_template(schema), indent=2, ensure_ascii=False),
            "",
            "Return JSON only. Do not add markdown fences or commentary.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"
