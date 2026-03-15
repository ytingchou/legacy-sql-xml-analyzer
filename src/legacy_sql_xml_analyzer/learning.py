from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


QUERY_TAGS = {"main-query", "sub-query"}
REFERENCE_TAGS = {"sql-refer-to", "ext-sql-refer-to", "sql-copy", "ext-sql-copy"}
REFERENCE_TOKEN_TEMPLATES = [
    "{name}",
    "/*{name}*/",
    "${name}",
    "{{{name}}}",
    "[{name}]",
    "@@{name}@@",
    "#{name}#",
]


@dataclass(slots=True)
class ProfileRule:
    rule_id: str
    rule_type: str
    description: str
    confidence: float
    evidence: dict[str, Any] = field(default_factory=dict)
    proposed_action: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_type": self.rule_type,
            "description": self.description,
            "confidence": round(self.confidence, 4),
            "evidence": self.evidence,
            "proposed_action": self.proposed_action,
        }


@dataclass(slots=True)
class AnalysisProfile:
    profile_version: int = 1
    profile_name: str | None = None
    profile_status: str = "candidate"
    parent_profile: str | None = None
    generated_at: str | None = None
    source_observation_digest: str | None = None
    reference_target_default_order: list[str] = field(default_factory=lambda: ["sub", "main"])
    reference_token_patterns: list[str] = field(default_factory=lambda: ["{name}"])
    external_xml_name_map: dict[str, str] = field(default_factory=dict)
    external_xml_scoped_map: dict[str, str] = field(default_factory=dict)
    ignore_tags: list[str] = field(default_factory=list)
    rules: list[ProfileRule] = field(default_factory=list)
    validation_history: list[dict[str, Any]] = field(default_factory=list)
    lifecycle_history: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_version": self.profile_version,
            "profile_name": self.profile_name,
            "profile_status": self.profile_status,
            "parent_profile": self.parent_profile,
            "generated_at": self.generated_at,
            "source_observation_digest": self.source_observation_digest,
            "reference_target_default_order": self.reference_target_default_order,
            "reference_token_patterns": self.reference_token_patterns,
            "external_xml_name_map": self.external_xml_name_map,
            "external_xml_scoped_map": self.external_xml_scoped_map,
            "ignore_tags": self.ignore_tags,
            "rules": [rule.to_dict() for rule in self.rules],
            "validation_history": self.validation_history,
            "lifecycle_history": self.lifecycle_history,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AnalysisProfile":
        rules = [
            ProfileRule(
                rule_id=item.get("rule_id", "unknown-rule"),
                rule_type=item.get("rule_type", "unknown"),
                description=item.get("description", ""),
                confidence=float(item.get("confidence", 0.0)),
                evidence=item.get("evidence", {}),
                proposed_action=item.get("proposed_action", {}),
            )
            for item in payload.get("rules", [])
        ]
        order = [item for item in payload.get("reference_target_default_order", ["sub", "main"]) if item in {"sub", "main"}]
        if not order:
            order = ["sub", "main"]
        token_patterns = [str(item) for item in payload.get("reference_token_patterns", ["{name}"]) if "{name}" in str(item)]
        if not token_patterns:
            token_patterns = ["{name}"]
        return cls(
            profile_version=int(payload.get("profile_version", 1)),
            profile_name=payload.get("profile_name"),
            profile_status=normalize_profile_status(payload.get("profile_status")),
            parent_profile=payload.get("parent_profile"),
            generated_at=payload.get("generated_at"),
            source_observation_digest=payload.get("source_observation_digest"),
            reference_target_default_order=order,
            reference_token_patterns=dedupe_preserve_order(token_patterns),
            external_xml_name_map={
                str(key): str(value) for key, value in payload.get("external_xml_name_map", {}).items()
            },
            external_xml_scoped_map={
                str(key): str(value) for key, value in payload.get("external_xml_scoped_map", {}).items()
            },
            ignore_tags=sorted({str(item) for item in payload.get("ignore_tags", [])}),
            rules=rules,
            validation_history=[
                item for item in payload.get("validation_history", []) if isinstance(item, dict)
            ],
            lifecycle_history=[
                item for item in payload.get("lifecycle_history", []) if isinstance(item, dict)
            ],
        )


def load_profile(path: Path | None) -> AnalysisProfile | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return AnalysisProfile.from_dict(payload)


def learn_directory(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    xml_files = sorted(input_dir.rglob("*.xml"))
    learning_root = output_dir / "learning"
    learning_root.mkdir(parents=True, exist_ok=True)

    files: list[dict[str, Any]] = []
    unknown_tags: Counter[str] = Counter()
    top_level_unknown_tags: Counter[str] = Counter()
    tag_frequencies: Counter[str] = Counter()
    attribute_frequencies: Counter[str] = Counter()
    implicit_local_refs: list[dict[str, Any]] = []
    external_xml_refs: list[dict[str, Any]] = []
    token_pattern_counts: Counter[str] = Counter()
    token_pattern_examples: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    query_registry: dict[tuple[Path, str], set[str]] = {}
    file_index: dict[Path, dict[str, Any]] = {}

    for xml_path in xml_files:
        resolved_path = xml_path.resolve()
        file_record = {
            "path": str(resolved_path),
            "parse_status": "ok",
            "root_tag": None,
            "tag_counts": {},
            "attribute_counts": {},
            "queries": {"main": [], "sub": []},
            "unsupported_tags": [],
            "top_level_unknown_tags": [],
        }
        files.append(file_record)
        file_index[resolved_path] = file_record
        try:
            root = ET.parse(resolved_path).getroot()
        except ET.ParseError as exc:
            file_record["parse_status"] = "failed"
            file_record["parse_error"] = str(exc)
            parse_errors.append({"path": str(resolved_path), "error": str(exc)})
            continue

        file_record["root_tag"] = root.tag
        local_main_names: set[str] = set()
        local_sub_names: set[str] = set()

        for element in root.iter():
            tag_frequencies[element.tag] += 1
            for attribute in element.attrib:
                attribute_frequencies[f"{element.tag}:{attribute}"] += 1
            if element.tag not in QUERY_TAGS | REFERENCE_TAGS | {"sql-mapping", "parameter", "sql-body"}:
                unknown_tags[element.tag] += 1
                file_record["unsupported_tags"].append(element.tag)

        for child in root:
            if child.tag not in QUERY_TAGS:
                if child.tag not in {"sql-mapping", "parameter", "sql-body"} | REFERENCE_TAGS:
                    top_level_unknown_tags[child.tag] += 1
                    file_record["top_level_unknown_tags"].append(child.tag)
                continue
            query_type = "main" if child.tag == "main-query" else "sub"
            query_name = (child.attrib.get("name") or "").strip()
            if not query_name:
                continue
            file_record["queries"][query_type].append(query_name)
            if query_type == "main":
                local_main_names.add(query_name)
            else:
                local_sub_names.add(query_name)

        query_registry[(resolved_path, "main")] = local_main_names
        query_registry[(resolved_path, "sub")] = local_sub_names
        file_record["tag_counts"] = dict(sorted(Counter(element.tag for element in root.iter()).items()))
        file_record["attribute_counts"] = dict(sorted(attribute_frequencies_for_root(root).items()))

    all_files = sorted(file_index)
    for resolved_path, file_record in file_index.items():
        if file_record["parse_status"] != "ok":
            continue
        root = ET.parse(resolved_path).getroot()
        for query_node in root:
            if query_node.tag not in QUERY_TAGS:
                continue
            owner_query_name = (query_node.attrib.get("name") or "").strip()
            sql_body_node = query_node.find("sql-body")
            raw_sql = (sql_body_node.text or "").strip() if sql_body_node is not None and sql_body_node.text else ""
            for nested in query_node:
                if nested.tag == "sql-refer-to":
                    name = (nested.attrib.get("name") or "").strip()
                    main_query = (nested.attrib.get("main-query") or "").strip()
                    sub_query = (nested.attrib.get("sub-query") or "").strip()
                    if not main_query and not sub_query and name:
                        implicit_local_refs.append(
                            {
                                "source_path": str(resolved_path),
                                "owner_query": owner_query_name,
                                "name": name,
                                "available_target_types": implicit_reference_candidates(
                                    query_registry, resolved_path, name
                                ),
                            }
                        )
                    record_reference_token_patterns(
                        token_pattern_counts,
                        token_pattern_examples,
                        reference_name=name,
                        raw_sql=raw_sql,
                        source_path=resolved_path,
                        owner_query=owner_query_name,
                    )
                elif nested.tag == "ext-sql-refer-to":
                    name = (nested.attrib.get("name") or "").strip()
                    xml_name = (nested.attrib.get("xml") or "").strip()
                    if xml_name:
                        source_dir = relative_dir_from_root(resolved_path.parent, input_dir.resolve())
                        external_xml_refs.append(
                            {
                                "source_path": str(resolved_path),
                                "source_dir": source_dir,
                                "owner_query": owner_query_name,
                                "xml_name": xml_name,
                                "direct_match_exists": (resolved_path.parent / xml_name).exists(),
                                "candidate_files": external_xml_candidates(
                                    xml_name=xml_name,
                                    source_path=resolved_path,
                                    input_dir=input_dir.resolve(),
                                    all_files=all_files,
                                ),
                            }
                        )
                    record_reference_token_patterns(
                        token_pattern_counts,
                        token_pattern_examples,
                        reference_name=name,
                        raw_sql=raw_sql,
                        source_path=resolved_path,
                        owner_query=owner_query_name,
                    )
                elif nested.tag in {"sql-copy", "ext-sql-copy"}:
                    name = (nested.attrib.get("name") or "").strip()
                    record_reference_token_patterns(
                        token_pattern_counts,
                        token_pattern_examples,
                        reference_name=name,
                        raw_sql=raw_sql,
                        source_path=resolved_path,
                        owner_query=owner_query_name,
                    )

    observations = {
        "generated_at": now_iso(),
        "input_dir": str(input_dir.resolve()),
        "summary": {
            "xml_file_count": len(xml_files),
            "parse_error_count": len(parse_errors),
            "unknown_tag_count": sum(unknown_tags.values()),
            "top_level_unknown_tag_count": sum(top_level_unknown_tags.values()),
            "implicit_local_reference_count": len(implicit_local_refs),
            "external_xml_reference_count": len(external_xml_refs),
        },
        "files": files,
        "parse_errors": parse_errors,
        "tag_frequencies": dict(sorted(tag_frequencies.items())),
        "attribute_frequencies": dict(sorted(attribute_frequencies.items())),
        "unknown_tags": dict(sorted(unknown_tags.items())),
        "top_level_unknown_tags": dict(sorted(top_level_unknown_tags.items())),
        "reference_patterns": {
            "implicit_local_refs": implicit_local_refs,
            "external_xml_refs": external_xml_refs,
            "token_patterns": dict(sorted(token_pattern_counts.items())),
            "token_pattern_examples": token_pattern_examples[:100],
        },
    }
    observations["observation_digest"] = digest_payload(observations)

    observations_path = learning_root / "observations.json"
    observations_path.write_text(json.dumps(observations, indent=2, ensure_ascii=False), encoding="utf-8")

    overview_path = learning_root / "overview.md"
    overview_path.write_text(render_observation_overview(observations), encoding="utf-8")
    return {
        "observations": observations,
        "artifacts": [str(observations_path), str(overview_path)],
    }


def infer_rules_from_observations(observations: dict[str, Any]) -> AnalysisProfile:
    rules: list[ProfileRule] = []
    external_xml_name_map: dict[str, str] = {}
    external_xml_scoped_map: dict[str, str] = {}
    ignore_tags: list[str] = []
    implicit_local_refs = observations.get("reference_patterns", {}).get("implicit_local_refs", [])
    external_xml_refs = observations.get("reference_patterns", {}).get("external_xml_refs", [])
    token_patterns = observations.get("reference_patterns", {}).get("token_patterns", {})
    top_level_unknown_tags = observations.get("top_level_unknown_tags", {})
    xml_file_count = int(observations.get("summary", {}).get("xml_file_count", 0))

    sub_only = sum(1 for item in implicit_local_refs if item.get("available_target_types") == ["sub"])
    main_only = sum(1 for item in implicit_local_refs if item.get("available_target_types") == ["main"])
    if main_only > sub_only:
        target_order = ["main", "sub"]
    else:
        target_order = ["sub", "main"]
    total_directional = sub_only + main_only
    default_order_confidence = (
        max(sub_only, main_only) / total_directional if total_directional else 0.5
    )
    rules.append(
        ProfileRule(
            rule_id="reference-target-default-order",
            rule_type="reference_target_default_order",
            description="Preferred lookup order for sql-refer-to nodes without main-query/sub-query.",
            confidence=default_order_confidence,
            evidence={"sub_only": sub_only, "main_only": main_only},
            proposed_action={"reference_target_default_order": target_order},
        )
    )

    inferred_token_patterns = ["{name}"]
    total_token_hits = sum(int(value) for value in token_patterns.values()) or 1
    for pattern, count in sorted(token_patterns.items(), key=lambda item: (-int(item[1]), item[0])):
        if pattern == "{name}" or int(count) <= 0:
            continue
        confidence = min(0.95, int(count) / total_token_hits + 0.35)
        inferred_token_patterns.append(pattern)
        rules.append(
            ProfileRule(
                rule_id=f"reference-token-pattern:{pattern}",
                rule_type="reference_token_pattern",
                description=f"Recognize wrapped reference token pattern {pattern}.",
                confidence=confidence,
                evidence={"observed_count": int(count), "total_token_hits": total_token_hits},
                proposed_action={"pattern": pattern},
            )
        )

    mapping_evidence: dict[str, list[str]] = {}
    for item in external_xml_refs:
        xml_name = item.get("xml_name")
        if not xml_name or item.get("direct_match_exists"):
            continue
        candidate_files = item.get("candidate_files", [])
        best_candidate = select_best_external_candidate(candidate_files)
        if best_candidate:
            mapping_evidence.setdefault(xml_name, []).append(best_candidate)

    for xml_name, candidates in sorted(mapping_evidence.items()):
        unique_candidates = sorted(set(candidates))
        if len(unique_candidates) == 1:
            external_xml_name_map[xml_name] = unique_candidates[0]

    for xml_name, mapped_name in sorted(external_xml_name_map.items()):
        rules.append(
            ProfileRule(
                rule_id=f"external-xml-map:{xml_name}",
                rule_type="external_xml_name_mapping",
                description=f"Map external xml alias '{xml_name}' to an observed file name.",
                confidence=0.95,
                evidence={"observed_targets": mapping_evidence.get(xml_name, [])},
                proposed_action={"xml_name": xml_name, "mapped_to": mapped_name},
            )
        )

    for item in external_xml_refs:
        xml_name = item.get("xml_name")
        source_dir = item.get("source_dir", ".")
        if not xml_name or item.get("direct_match_exists") or xml_name in external_xml_name_map:
            continue
        best_candidate = select_best_external_candidate(item.get("candidate_files", []))
        if not best_candidate:
            continue
        scoped_key = scoped_external_key(source_dir, xml_name)
        external_xml_scoped_map[scoped_key] = best_candidate

    for scoped_key, mapped_name in sorted(external_xml_scoped_map.items()):
        source_dir, xml_name = scoped_key.split("::", 1)
        rules.append(
            ProfileRule(
                rule_id=f"external-xml-scoped-map:{scoped_key}",
                rule_type="external_xml_scoped_mapping",
                description=(
                    f"Map external xml alias '{xml_name}' from source dir '{source_dir}' to '{mapped_name}'."
                ),
                confidence=0.9,
                evidence={"source_dir": source_dir, "mapped_to": mapped_name},
                proposed_action={"source_dir": source_dir, "xml_name": xml_name, "mapped_to": mapped_name},
            )
        )

    for tag, count in sorted(top_level_unknown_tags.items(), key=lambda item: (-int(item[1]), item[0])):
        count_int = int(count)
        if count_int < 2 and xml_file_count > 1:
            continue
        confidence = min(0.95, (count_int / max(xml_file_count, 1)) * 0.8 + 0.2)
        ignore_tags.append(tag)
        rules.append(
            ProfileRule(
                rule_id=f"ignore-tag:{tag}",
                rule_type="ignore_tag",
                description=f"Ignore repeated top-level custom tag <{tag}> during parsing.",
                confidence=confidence,
                evidence={"observed_count": count_int, "xml_file_count": xml_file_count},
                proposed_action={"tag": tag},
            )
        )

    profile = AnalysisProfile(
        generated_at=now_iso(),
        source_observation_digest=observations.get("observation_digest"),
        reference_target_default_order=target_order,
        reference_token_patterns=dedupe_preserve_order(inferred_token_patterns),
        external_xml_name_map=external_xml_name_map,
        external_xml_scoped_map=external_xml_scoped_map,
        ignore_tags=sorted(ignore_tags),
        rules=rules,
    )
    return profile


def infer_rules(input_path: Path, output_dir: Path) -> dict[str, Any]:
    observations_path = resolve_observations_path(input_path)
    observations = json.loads(observations_path.read_text(encoding="utf-8"))
    profile = infer_rules_from_observations(observations)

    learning_root = output_dir / "learning"
    learning_root.mkdir(parents=True, exist_ok=True)
    rules_path = learning_root / "rule_candidates.json"
    rules_path.write_text(json.dumps(profile.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    summary_path = learning_root / "rule_candidates.md"
    summary_path.write_text(render_rule_summary(profile), encoding="utf-8")
    return {
        "profile": profile,
        "artifacts": [str(rules_path), str(summary_path)],
    }


def freeze_profile(input_path: Path, output_path: Path, min_confidence: float = 0.8) -> AnalysisProfile:
    profile_payload = json.loads(input_path.read_text(encoding="utf-8"))
    profile = AnalysisProfile.from_dict(profile_payload)
    retained_rules = [rule for rule in profile.rules if rule.confidence >= min_confidence]
    retained_token_patterns = ["{name}"]
    retained_external_xml_map: dict[str, str] = {}
    retained_external_xml_scoped_map: dict[str, str] = {}
    retained_ignore_tags: list[str] = []
    retained_target_order = ["sub", "main"]

    for rule in retained_rules:
        if rule.rule_type == "reference_target_default_order":
            order = rule.proposed_action.get("reference_target_default_order", [])
            retained_target_order = [item for item in order if item in {"sub", "main"}] or ["sub", "main"]
        elif rule.rule_type == "reference_token_pattern":
            pattern = str(rule.proposed_action.get("pattern", ""))
            if "{name}" in pattern:
                retained_token_patterns.append(pattern)
        elif rule.rule_type == "external_xml_name_mapping":
            xml_name = str(rule.proposed_action.get("xml_name", ""))
            mapped_to = str(rule.proposed_action.get("mapped_to", ""))
            if xml_name and mapped_to:
                retained_external_xml_map[xml_name] = mapped_to
        elif rule.rule_type == "external_xml_scoped_mapping":
            source_dir = str(rule.proposed_action.get("source_dir", ""))
            xml_name = str(rule.proposed_action.get("xml_name", ""))
            mapped_to = str(rule.proposed_action.get("mapped_to", ""))
            if xml_name and mapped_to:
                retained_external_xml_scoped_map[scoped_external_key(source_dir, xml_name)] = mapped_to
        elif rule.rule_type == "ignore_tag":
            tag = str(rule.proposed_action.get("tag", ""))
            if tag:
                retained_ignore_tags.append(tag)

    frozen_profile = AnalysisProfile(
        generated_at=now_iso(),
        source_observation_digest=profile.source_observation_digest,
        reference_target_default_order=retained_target_order,
        reference_token_patterns=dedupe_preserve_order(retained_token_patterns),
        external_xml_name_map=retained_external_xml_map,
        external_xml_scoped_map=retained_external_xml_scoped_map,
        ignore_tags=sorted(set(retained_ignore_tags)),
        rules=retained_rules,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(frozen_profile.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return frozen_profile


def resolve_observations_path(input_path: Path) -> Path:
    if input_path.is_dir():
        candidate = input_path / "observations.json"
        if candidate.exists():
            return candidate
        learning_candidate = input_path / "learning" / "observations.json"
        if learning_candidate.exists():
            return learning_candidate
    return input_path


def implicit_reference_candidates(
    query_registry: dict[tuple[Path, str], set[str]],
    source_path: Path,
    name: str,
) -> list[str]:
    candidates: list[str] = []
    if name in query_registry.get((source_path, "sub"), set()):
        candidates.append("sub")
    if name in query_registry.get((source_path, "main"), set()):
        candidates.append("main")
    return candidates


def external_xml_candidates(
    xml_name: str,
    source_path: Path,
    input_dir: Path,
    all_files: list[Path],
) -> list[dict[str, Any]]:
    direct_path = (source_path.parent / xml_name).resolve()
    candidates: list[dict[str, Any]] = []
    for file_path in all_files:
        score = external_xml_candidate_score(xml_name, source_path, input_dir, file_path, direct_path)
        if score <= 0:
            continue
        candidates.append(
            {
                "relative_path": relative_path_from_root(file_path, input_dir),
                "score": score,
            }
        )
    candidates.sort(key=lambda item: (-int(item["score"]), item["relative_path"]))
    return candidates


def external_xml_candidate_score(
    xml_name: str,
    source_path: Path,
    input_dir: Path,
    file_path: Path,
    direct_path: Path,
) -> int:
    alias = xml_name.strip().lower()
    file_name = file_path.name.lower()
    stem = file_path.stem.lower()
    relative_path = relative_path_from_root(file_path, input_dir).lower()
    score = 0

    if file_path == direct_path:
        return 200
    if alias == file_name:
        score = max(score, 180)
    if alias == stem:
        score = max(score, 170)
    if file_name.startswith(f"{alias}."):
        score = max(score, 165)
    if stem.startswith(alias):
        score = max(score, 155)
    if relative_path.endswith(f"/{alias}") or relative_path.endswith(f"/{alias}.xml"):
        score = max(score, 150)
    if f"/{alias}/" in relative_path or relative_path.startswith(f"{alias}/"):
        score = max(score, 140)
    if alias in relative_path:
        score = max(score, 120)

    source_parent = source_path.parent.resolve()
    if score > 0 and file_path.parent.resolve() == source_parent:
        score += 8
    if score > 0:
        score += max(0, 6 - path_distance(source_parent, file_path.parent.resolve()))
        score += shared_prefix_depth(source_parent, file_path.parent.resolve()) * 6
    return score


def path_distance(left: Path, right: Path) -> int:
    left_parts = left.parts
    right_parts = right.parts
    shared = 0
    for left_part, right_part in zip(left_parts, right_parts):
        if left_part != right_part:
            break
        shared += 1
    return (len(left_parts) - shared) + (len(right_parts) - shared)


def shared_prefix_depth(left: Path, right: Path) -> int:
    shared = 0
    for left_part, right_part in zip(left.parts, right.parts):
        if left_part != right_part:
            break
        shared += 1
    return shared


def relative_path_from_root(path: Path, input_dir: Path) -> str:
    return path.resolve().relative_to(input_dir.resolve()).as_posix()


def relative_dir_from_root(path: Path, input_dir: Path) -> str:
    relative = path.resolve().relative_to(input_dir.resolve()).as_posix()
    return relative or "."


def attribute_frequencies_for_root(root: ET.Element) -> Counter[str]:
    counts: Counter[str] = Counter()
    for element in root.iter():
        for attribute in element.attrib:
            counts[f"{element.tag}:{attribute}"] += 1
    return counts


def digest_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def render_observation_overview(observations: dict[str, Any]) -> str:
    summary = observations.get("summary", {})
    lines = [
        "# Learning Overview",
        "",
        "## Summary",
        f"- XML files: {summary.get('xml_file_count', 0)}",
        f"- Parse errors: {summary.get('parse_error_count', 0)}",
        f"- Unknown tag hits: {summary.get('unknown_tag_count', 0)}",
        f"- Top-level unknown tags: {summary.get('top_level_unknown_tag_count', 0)}",
        f"- Implicit local refs: {summary.get('implicit_local_reference_count', 0)}",
        f"- External xml refs: {summary.get('external_xml_reference_count', 0)}",
        "",
        "## Top Unknown Tags",
    ]
    unknown_tags = observations.get("unknown_tags", {})
    if unknown_tags:
        for tag, count in sorted(unknown_tags.items(), key=lambda item: item[1], reverse=True)[:10]:
            lines.append(f"- `{tag}`: {count}")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def render_rule_summary(profile: AnalysisProfile) -> str:
    lines = [
        "# Rule Candidates",
        "",
        "## Profile Defaults",
        f"- Reference target order: `{', '.join(profile.reference_target_default_order)}`",
        f"- Reference token patterns: `{', '.join(profile.reference_token_patterns)}`",
        f"- External xml mappings: {len(profile.external_xml_name_map)}",
        f"- External xml scoped mappings: {len(profile.external_xml_scoped_map)}",
        f"- Ignore tags: {', '.join(profile.ignore_tags) or 'none'}",
        "",
        "## Rules",
    ]
    for rule in profile.rules:
        lines.append(
            f"- `{rule.rule_id}` type={rule.rule_type} confidence={rule.confidence:.2f}: {rule.description}"
        )
    return "\n".join(lines).rstrip() + "\n"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_profile_status(value: Any) -> str:
    status = str(value or "candidate").strip().lower()
    if status not in {"candidate", "trial", "trusted", "deprecated"}:
        return "candidate"
    return status


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def record_reference_token_patterns(
    token_pattern_counts: Counter[str],
    token_pattern_examples: list[dict[str, Any]],
    reference_name: str,
    raw_sql: str,
    source_path: Path,
    owner_query: str,
) -> None:
    if not reference_name or not raw_sql:
        return
    matched_patterns = [
        pattern
        for pattern in REFERENCE_TOKEN_TEMPLATES
        if pattern.replace("{name}", reference_name) in raw_sql
    ]
    for pattern in matched_patterns:
        token_pattern_counts[pattern] += 1
    if matched_patterns:
        token_pattern_examples.append(
            {
                "source_path": str(source_path),
                "owner_query": owner_query,
                "reference_name": reference_name,
                "matched_patterns": matched_patterns,
            }
        )


def select_best_external_candidate(candidate_files: list[Any]) -> str | None:
    normalized: list[tuple[str, int]] = []
    for item in candidate_files:
        if isinstance(item, dict):
            path = str(item.get("relative_path", ""))
            score = int(item.get("score", 0))
        else:
            path = str(item)
            score = 0
        if path:
            normalized.append((path, score))
    if not normalized:
        return None
    normalized.sort(key=lambda item: (-item[1], item[0]))
    best_path, best_score = normalized[0]
    if len(normalized) == 1:
        return best_path
    second_score = normalized[1][1]
    if best_score >= second_score + 5:
        return best_path
    return None


def scoped_external_key(source_dir: str, xml_name: str) -> str:
    return f"{source_dir}::{xml_name}"
