from __future__ import annotations

import ast
import json
import re
from dataclasses import asdict, dataclass
from typing import Any


TRAILING_COMMA_RE = re.compile(r",(\s*[}\]])")
FENCE_RE = re.compile(r"^```[A-Za-z0-9_-]*\s*$")


@dataclass(slots=True)
class NormalizationResult:
    raw_text: str
    normalized_text: str
    normalized_object: dict[str, Any] | None
    source: str
    source_type: str
    applied_steps: list[str]
    warnings: list[str]
    confidence: str
    parse_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_response(raw_text: str, *, source: str = "generic") -> NormalizationResult:
    text = raw_text.strip()
    applied_steps: list[str] = []
    warnings: list[str] = []

    if not text:
        return NormalizationResult(
            raw_text=raw_text,
            normalized_text="",
            normalized_object=None,
            source=source,
            source_type="empty",
            applied_steps=[],
            warnings=["response_was_empty"],
            confidence="low",
            parse_error="empty_response",
        )

    direct = try_load_json_object(text)
    if direct is not None:
        return NormalizationResult(
            raw_text=raw_text,
            normalized_text=text,
            normalized_object=direct,
            source=source,
            source_type="json_object",
            applied_steps=applied_steps,
            warnings=warnings,
            confidence="high",
        )

    cline_text = extract_cline_json_response(text)
    if cline_text:
        applied_steps.append("extracted_cline_json_response")
        nested = normalize_response(cline_text, source=f"{source}:cline-json")
        nested.applied_steps = applied_steps + nested.applied_steps
        nested.source = source
        nested.source_type = "cline_json_event_stream"
        if nested.confidence == "high":
            nested.confidence = "medium"
        return nested

    unfenced = strip_markdown_fences(text)
    if unfenced != text:
        applied_steps.append("removed_markdown_fences")
        direct = try_load_json_object(unfenced)
        if direct is not None:
            return NormalizationResult(
                raw_text=raw_text,
                normalized_text=unfenced,
                normalized_object=direct,
                source=source,
                source_type="fenced_json",
                applied_steps=applied_steps,
                warnings=warnings,
                confidence="medium",
            )
        text = unfenced

    outer = trim_outer_object(text)
    if outer and outer != text:
        applied_steps.append("trimmed_outer_text")
        direct = try_load_json_object(outer)
        if direct is not None:
            return NormalizationResult(
                raw_text=raw_text,
                normalized_text=outer,
                normalized_object=direct,
                source=source,
                source_type="trimmed_outer_json",
                applied_steps=applied_steps,
                warnings=warnings,
                confidence="medium",
            )
        text = outer

    literal_payload = parse_python_literal_object(text)
    if literal_payload is not None:
        applied_steps.append("parsed_python_literal")
        normalized_text = json.dumps(literal_payload, ensure_ascii=False, indent=2)
        return NormalizationResult(
            raw_text=raw_text,
            normalized_text=normalized_text,
            normalized_object=literal_payload,
            source=source,
            source_type="python_literal",
            applied_steps=applied_steps,
            warnings=warnings,
            confidence="medium",
        )

    repaired = repair_common_json_issues(text)
    if repaired != text:
        applied_steps.append("repaired_common_json_issues")
        direct = try_load_json_object(repaired)
        if direct is not None:
            return NormalizationResult(
                raw_text=raw_text,
                normalized_text=repaired,
                normalized_object=direct,
                source=source,
                source_type="repaired_json",
                applied_steps=applied_steps,
                warnings=warnings,
                confidence="low",
            )

    return NormalizationResult(
        raw_text=raw_text,
        normalized_text=text,
        normalized_object=None,
        source=source,
        source_type="unparsed_text",
        applied_steps=applied_steps,
        warnings=warnings,
        confidence="low",
        parse_error="could_not_normalize_to_object",
    )


def normalize_response_text(raw_text: str, *, source: str = "generic") -> tuple[str, list[str]]:
    result = normalize_response(raw_text, source=source)
    return result.normalized_text, result.applied_steps


def strip_markdown_fences(text: str) -> str:
    lines = text.strip().splitlines()
    if len(lines) >= 2 and FENCE_RE.match(lines[0].strip()) and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return text


def trim_outer_object(text: str) -> str | None:
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        return text[start : end + 1].strip()
    return None


def repair_common_json_issues(text: str) -> str:
    repaired = text.strip()
    repaired = TRAILING_COMMA_RE.sub(r"\1", repaired)
    return repaired


def try_load_json_object(text: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def parse_python_literal_object(text: str) -> dict[str, Any] | None:
    try:
        payload = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def extract_cline_json_response(text: str) -> str | None:
    events = parse_json_events(text)
    candidates: list[str] = []
    for event in events:
        extracted = extract_assistant_text_from_event(event)
        if extracted:
            candidates.append(extracted)
    return candidates[-1] if candidates else None


def parse_json_events(text: str) -> list[Any]:
    stripped = text.strip()
    if not stripped:
        return []
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]

    events: list[Any] = []
    for line in stripped.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            events.append(json.loads(candidate))
        except json.JSONDecodeError:
            continue
    return events


def extract_assistant_text_from_event(event: Any) -> str | None:
    if not isinstance(event, dict):
        return None
    role_markers = " ".join(
        str(event.get(key) or "")
        for key in ("role", "type", "kind", "event", "name")
    ).lower()
    if role_markers and any(token in role_markers for token in ("tool", "system", "user", "status", "progress")):
        if not any(token in role_markers for token in ("assistant", "result", "response", "final", "completion")):
            return None

    candidates: list[str] = []
    for key in ("message", "data", "result", "response", "payload", "delta"):
        value = event.get(key)
        extracted = extract_text_blob(value)
        if extracted:
            candidates.append(extracted)
    direct = extract_text_blob(event)
    if direct:
        candidates.append(direct)
    return candidates[-1] if candidates else None


def extract_text_blob(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, list):
        parts = [part for item in value if (part := extract_text_blob(item))]
        return "\n".join(parts).strip() or None
    if isinstance(value, dict):
        role = str(value.get("role") or "").lower()
        if role in {"tool", "system", "user"}:
            return None
        if "text" in value and isinstance(value["text"], str):
            stripped = value["text"].strip()
            if stripped:
                return stripped
        if "content" in value:
            content = extract_text_blob(value["content"])
            if content:
                return content
    return None
