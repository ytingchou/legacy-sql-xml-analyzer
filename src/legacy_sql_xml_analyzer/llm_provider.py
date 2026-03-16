from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request

from .analyzer import append_artifacts_to_index
from .dashboard import write_evolution_report
from .evolution import review_llm_response_from_analysis
from .prompting import (
    artifact_descriptor_for_path,
    prepare_prompt_pack_from_analysis,
    prompt_pack_text_path,
    resolve_analysis_root,
    sanitize_token,
)


DEFAULT_SYSTEM_PROMPT = (
    "You are a careful assistant for a legacy SQL XML analyzer. "
    "Always follow the prompt instructions exactly. "
    "If asked for JSON, return valid JSON only."
)
DEFAULT_PROVIDER_VALIDATION_PROMPT = (
    'Return JSON only with this exact shape: {"provider_ok": true, "echo": "provider-validation"}. '
    "Do not wrap the JSON in markdown."
)


@dataclass(slots=True)
class OpenAICompatibleConfig:
    base_url: str
    model: str
    api_key: str
    token_limit: int = 2048
    temperature: float = 0.0
    timeout_seconds: float = 60.0
    provider_name: str | None = None
    api_key_env: str = "OPENAI_API_KEY"
    headers: dict[str, str] = field(default_factory=dict)
    system_prompt: str = DEFAULT_SYSTEM_PROMPT


class LlmProviderError(RuntimeError):
    pass


def validate_provider_connection(
    output_dir: Path,
    provider_config_path: Path | None = None,
    provider_base_url: str | None = None,
    provider_api_key: str | None = None,
    provider_api_key_env: str = "OPENAI_API_KEY",
    provider_model: str | None = None,
    provider_name: str | None = None,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout_seconds: float | None = None,
    prompt_text: str | None = None,
    expect_json: bool = True,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    validation_root = output_dir / "analysis" / "provider_validation"
    validation_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_token = sanitize_token(provider_name or provider_model or "provider")
    run_dir = validation_root / f"{timestamp}-{run_token}"
    run_dir.mkdir(parents=True, exist_ok=True)

    resolved_prompt = str(prompt_text or DEFAULT_PROVIDER_VALIDATION_PROMPT)
    input_debug = {
        "provider_config_path": str(provider_config_path.resolve()) if provider_config_path else None,
        "provider_base_url": provider_base_url,
        "provider_api_key_supplied": bool(provider_api_key),
        "provider_api_key_env": provider_api_key_env,
        "provider_api_key_env_present": bool(os.environ.get(provider_api_key_env or "")),
        "provider_model": provider_model,
        "provider_name": provider_name,
        "token_limit": token_limit,
        "temperature": temperature,
        "timeout_seconds": timeout_seconds,
        "expect_json": expect_json,
    }
    debug_payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "validation_id": run_dir.name,
        "status": "running",
        "input": input_debug,
        "prompt_preview": resolved_prompt[:500],
        "prompt_estimated_tokens": max(1, round(len(resolved_prompt) / 4)),
        "request_path": None,
        "response_json_path": None,
        "response_text_path": None,
        "checks": [],
        "error": None,
        "troubleshooting_hints": [],
    }
    summary: dict[str, Any] = {
        "generated_at": debug_payload["generated_at"],
        "validation_id": run_dir.name,
        "status": "failed",
        "provider_name": provider_name or provider_model or "unknown",
        "provider_model": provider_model or "unknown",
        "provider_base_url": provider_base_url or "unknown",
        "normalized_url": normalize_chat_completions_url(provider_base_url or "https://invalid-provider.local")
        if provider_base_url
        else None,
        "token_limit": token_limit,
        "temperature": temperature,
        "timeout_seconds": timeout_seconds,
        "expect_json": expect_json,
        "checks": [],
        "error": None,
        "troubleshooting_hints": [],
    }

    request_path = run_dir / "request.json"
    response_json_path = run_dir / "response.json"
    response_text_path = run_dir / "response.txt"
    debug_path = run_dir / "debug.json"
    summary_path = run_dir / "summary.json"
    summary_md_path = run_dir / "summary.md"

    artifacts: list[Any] = []
    try:
        config = resolve_provider_config(
            provider_config_path=provider_config_path,
            provider_base_url=provider_base_url,
            provider_api_key=provider_api_key,
            provider_api_key_env=provider_api_key_env,
            provider_model=provider_model,
            provider_name=provider_name,
            token_limit=token_limit,
            temperature=temperature,
            timeout_seconds=timeout_seconds,
        )
        summary.update(
            {
                "provider_name": config.provider_name or config.model,
                "provider_model": config.model,
                "provider_base_url": config.base_url,
                "normalized_url": normalize_chat_completions_url(config.base_url),
                "token_limit": config.token_limit,
                "temperature": config.temperature,
                "timeout_seconds": config.timeout_seconds,
            }
        )
        debug_payload["resolved_config"] = build_provider_debug_snapshot(config, resolved_prompt)
        debug_payload["checks"].append({"name": "config_resolution", "status": "passed", "detail": "Provider configuration resolved successfully."})

        request_artifact = build_request_artifact(config, resolved_prompt)
        request_path.write_text(json.dumps(request_artifact, indent=2, ensure_ascii=False), encoding="utf-8")
        debug_payload["request_path"] = str(request_path)
        debug_payload["checks"].append({"name": "request_artifact", "status": "passed", "detail": "Sanitized request artifact written before the live provider probe."})

        response_payload = post_chat_completion(config=config, prompt_text=resolved_prompt)
        response_json_path.write_text(json.dumps(response_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        debug_payload["response_json_path"] = str(response_json_path)
        debug_payload["checks"].append({"name": "chat_completions", "status": "passed", "detail": "Provider accepted the OpenAI-compatible chat/completions request."})

        response_text = extract_response_text(response_payload)
        response_text_path.write_text(response_text, encoding="utf-8")
        debug_payload["response_text_path"] = str(response_text_path)
        debug_payload["response_text_preview"] = response_text[:500]
        debug_payload["checks"].append({"name": "response_text", "status": "passed", "detail": "choices[0].message.content was extracted successfully."})

        warnings: list[dict[str, Any]] = []
        if expect_json:
            try:
                parsed = json.loads(response_text)
            except json.JSONDecodeError as exc:
                warnings.append(
                    {
                        "name": "json_echo",
                        "status": "warning",
                        "detail": f"Provider returned text but not valid JSON: {exc.msg}.",
                    }
                )
                debug_payload["response_json_parse_error"] = exc.msg
            else:
                debug_payload["parsed_response"] = parsed
                warnings.append({"name": "json_echo", "status": "passed", "detail": "Provider returned valid JSON for the probe prompt."})

        all_checks = [*debug_payload["checks"], *warnings]
        status = "passed_with_warnings" if any(item["status"] == "warning" for item in warnings) else "passed"
        troubleshooting_hints = build_provider_troubleshooting_hints(
            error_message=None,
            normalized_url=summary["normalized_url"],
            expect_json=expect_json,
            status=status,
        )

        summary.update(
            {
                "status": status,
                "checks": all_checks,
                "error": None,
                "response_usage": response_payload.get("usage", {}),
                "response_id": response_payload.get("id"),
                "response_text_path": str(response_text_path),
                "response_json_path": str(response_json_path),
                "request_path": str(request_path),
                "troubleshooting_hints": troubleshooting_hints,
            }
        )
        debug_payload["status"] = status
        debug_payload["checks"] = all_checks
        debug_payload["troubleshooting_hints"] = troubleshooting_hints
    except Exception as exc:
        error_message = str(exc)
        error_payload = {
            "type": type(exc).__name__,
            "message": error_message,
            "category": classify_provider_error(error_message),
        }
        failed_checks = [*debug_payload["checks"], {"name": "live_probe", "status": "failed", "detail": error_message}]
        troubleshooting_hints = build_provider_troubleshooting_hints(
            error_message=error_message,
            normalized_url=summary.get("normalized_url"),
            expect_json=expect_json,
            status="failed",
        )
        summary.update(
            {
                "status": "failed",
                "checks": failed_checks,
                "error": error_payload,
                "request_path": str(request_path) if request_path.exists() else None,
                "response_json_path": str(response_json_path) if response_json_path.exists() else None,
                "response_text_path": str(response_text_path) if response_text_path.exists() else None,
                "troubleshooting_hints": troubleshooting_hints,
            }
        )
        debug_payload["status"] = "failed"
        debug_payload["checks"] = failed_checks
        debug_payload["error"] = error_payload
        debug_payload["troubleshooting_hints"] = troubleshooting_hints

    debug_path.write_text(json.dumps(debug_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    summary["debug_path"] = str(debug_path)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    summary_md_path.write_text(render_provider_validation_markdown(summary), encoding="utf-8")

    artifacts.extend(
        [
            artifact_descriptor_for_path(summary_path, "json", f"Provider validation summary: {summary['provider_name']}", "provider"),
            artifact_descriptor_for_path(summary_md_path, "markdown", f"Provider validation summary (Markdown): {summary['provider_name']}", "provider"),
            artifact_descriptor_for_path(debug_path, "json", f"Provider validation debug: {summary['provider_name']}", "provider"),
        ]
    )
    if request_path.exists():
        artifacts.append(artifact_descriptor_for_path(request_path, "json", f"Provider validation request: {summary['provider_name']}", "provider"))
    if response_json_path.exists():
        artifacts.append(artifact_descriptor_for_path(response_json_path, "json", f"Provider validation response JSON: {summary['provider_name']}", "provider"))
    if response_text_path.exists():
        artifacts.append(artifact_descriptor_for_path(response_text_path, "text", f"Provider validation response text: {summary['provider_name']}", "provider"))
    append_artifacts_to_index(output_dir, artifacts)
    return {"summary": summary, "artifacts": artifacts}


def invoke_llm_from_analysis(
    analysis_root: Path,
    cluster_id: str,
    stage: str = "propose",
    budget: str = "128k",
    prompt_model: str = "weak-128k",
    provider_config_path: Path | None = None,
    provider_base_url: str | None = None,
    provider_api_key: str | None = None,
    provider_api_key_env: str = "OPENAI_API_KEY",
    provider_model: str | None = None,
    provider_name: str | None = None,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout_seconds: float | None = None,
    review: bool = False,
    profile_path: Path | None = None,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    config = resolve_provider_config(
        provider_config_path=provider_config_path,
        provider_base_url=provider_base_url,
        provider_api_key=provider_api_key,
        provider_api_key_env=provider_api_key_env,
        provider_model=provider_model,
        provider_name=provider_name,
        token_limit=token_limit,
        temperature=temperature,
        timeout_seconds=timeout_seconds,
    )

    prepare_prompt_pack_from_analysis(
        analysis_root=analysis_root,
        cluster_id=cluster_id,
        budget=budget,
        model=prompt_model,
    )
    prompt_root = analysis_root / "prompt_packs"
    prompt_path = prompt_pack_text_path(prompt_root, cluster_id, budget, prompt_model, stage)
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt pack not found: {prompt_path}")
    prompt_text = prompt_path.read_text(encoding="utf-8")

    response_payload = post_chat_completion(
        config=config,
        prompt_text=prompt_text,
    )
    response_text = extract_response_text(response_payload)
    run_summary = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "cluster_id": cluster_id,
        "stage": stage,
        "budget": budget,
        "prompt_model": prompt_model,
        "provider_name": config.provider_name or config.model,
        "provider_base_url": normalize_chat_completions_url(config.base_url),
        "provider_model": config.model,
        "token_limit": config.token_limit,
        "temperature": config.temperature,
        "timeout_seconds": config.timeout_seconds,
        "prompt_path": str(prompt_path),
        "prompt_sha256": hashlib.sha256(prompt_text.encode("utf-8")).hexdigest(),
        "prompt_estimated_tokens": max(1, round(len(prompt_text) / 4)),
        "response_usage": response_payload.get("usage", {}),
        "response_id": response_payload.get("id"),
        "review_enabled": review,
    }

    run_root = analysis_root / "llm_runs"
    run_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = run_root / f"{timestamp}-{cluster_id}-{stage}-{sanitize_token(config.provider_name or config.model)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    request_path = run_dir / "request.json"
    response_json_path = run_dir / "response.json"
    response_text_path = run_dir / "response.txt"
    summary_path = run_dir / "run_summary.json"
    summary_md_path = run_dir / "run_summary.md"

    request_path.write_text(
        json.dumps(build_request_artifact(config, prompt_text), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    response_json_path.write_text(json.dumps(response_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    response_text_path.write_text(response_text, encoding="utf-8")
    summary_path.write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    summary_md_path.write_text(render_run_summary_markdown(run_summary), encoding="utf-8")

    run_artifacts = [
        artifact_descriptor_for_path(request_path, "json", f"LLM request: {cluster_id}/{stage}", "prompting"),
        artifact_descriptor_for_path(response_json_path, "json", f"LLM response JSON: {cluster_id}/{stage}", "prompting"),
        artifact_descriptor_for_path(response_text_path, "text", f"LLM response text: {cluster_id}/{stage}", "prompting"),
        artifact_descriptor_for_path(summary_path, "json", f"LLM run summary: {cluster_id}/{stage}", "prompting"),
        artifact_descriptor_for_path(summary_md_path, "markdown", f"LLM run summary (Markdown): {cluster_id}/{stage}", "prompting"),
    ]
    all_artifacts = list(run_artifacts)

    review_result = None
    if review:
        review_result = review_llm_response_from_analysis(
            analysis_root=analysis_root,
            cluster_id=cluster_id,
            response_path=response_text_path,
            stage=stage,
            budget=budget,
            model=prompt_model,
            profile_path=profile_path,
        )
        all_artifacts.extend(review_result["artifacts"])
        run_summary["review_status"] = review_result["review"]["status"]
        run_summary["review_path"] = review_result["review"].get("response_path")
        summary_path.write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")
        summary_md_path.write_text(render_run_summary_markdown(run_summary), encoding="utf-8")

    append_run_summary_to_index(run_root, summary_path, run_summary)
    evolution_artifacts = write_evolution_report(analysis_root.parent)
    all_artifacts.extend(evolution_artifacts)
    append_artifacts_to_index(analysis_root.parent, run_artifacts)
    append_artifacts_to_index(analysis_root.parent, evolution_artifacts)
    return {
        "run_summary": run_summary,
        "response_text": response_text,
        "response_payload": response_payload,
        "artifacts": all_artifacts,
        "review": review_result,
    }


def build_provider_debug_snapshot(config: OpenAICompatibleConfig, prompt_text: str) -> dict[str, Any]:
    return {
        "provider_name": config.provider_name or config.model,
        "provider_model": config.model,
        "provider_base_url": config.base_url,
        "normalized_url": normalize_chat_completions_url(config.base_url),
        "api_key_env": config.api_key_env,
        "api_key_present": bool(config.api_key),
        "header_names": sorted(["Content-Type", "Accept", "Authorization", *config.headers.keys()]),
        "token_limit": config.token_limit,
        "temperature": config.temperature,
        "timeout_seconds": config.timeout_seconds,
        "system_prompt_present": bool(config.system_prompt),
        "prompt_estimated_tokens": max(1, round(len(prompt_text) / 4)),
    }


def classify_provider_error(message: str) -> str:
    lowered = message.lower()
    if "api key" in lowered or "401" in lowered or "403" in lowered:
        return "authentication"
    if "base url" in lowered or "404" in lowered:
        return "endpoint"
    if "rate-limit" in lowered or "rate limited" in lowered or "429" in lowered:
        return "rate_limit"
    if "non-json" in lowered or "json" in lowered and "returned" in lowered:
        return "response_format"
    if "missing choices" in lowered or "message.content" in lowered:
        return "response_shape"
    if "failed to reach" in lowered or "network" in lowered or "timed out" in lowered:
        return "network"
    if "token limit" in lowered or "max_tokens" in lowered:
        return "token_limit"
    if "timeout" in lowered:
        return "timeout"
    return "unknown"


def build_provider_troubleshooting_hints(
    error_message: str | None,
    normalized_url: str | None,
    expect_json: bool,
    status: str,
) -> list[str]:
    hints: list[str] = []
    if normalized_url:
        hints.append(f"Confirm the provider root resolves to `{normalized_url}` and that the endpoint is OpenAI-compatible.")
    if expect_json:
        hints.append("Use a short probe prompt that requests JSON only so response-shape issues are easy to isolate.")
    if status == "passed_with_warnings":
        hints.append("The provider is reachable, but the model did not follow the JSON probe exactly; tighten system prompt or lower temperature.")
    lowered = (error_message or "").lower()
    if "api key" in lowered or "401" in lowered or "403" in lowered:
        hints.append("Check whether the API key is present in the configured env var and whether the provider expects Bearer authentication.")
    if "404" in lowered or "base url" in lowered:
        hints.append("Use the provider /v1 root instead of a nested path unless the vendor documents a different OpenAI-compatible route.")
    if "failed to reach" in lowered or "network" in lowered:
        hints.append("Verify outbound network access, DNS resolution, and TLS interception rules in the company environment.")
    if "429" in lowered:
        hints.append("Reduce concurrency or switch to a model quota that allows the current request rate.")
    if "non-json" in lowered or "response_format" in lowered:
        hints.append("Inspect the saved response preview to confirm whether the provider returned HTML, plaintext, or a vendor-specific envelope.")
    if "missing choices" in lowered or "message.content" in lowered:
        hints.append("The provider response is not OpenAI-compatible enough; compare its JSON envelope against choices[0].message.content.")
    if not hints:
        hints.append("Inspect the saved debug.json and summary.json artifacts for the exact request settings and failure classification.")
    return hints


def render_provider_validation_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Provider Validation Summary",
        "",
        f"- Status: `{summary['status']}`",
        f"- Provider: `{summary['provider_name']}`",
        f"- Model: `{summary['provider_model']}`",
        f"- URL: `{summary.get('normalized_url') or summary.get('provider_base_url') or 'n/a'}`",
        f"- Token limit: `{summary.get('token_limit')}`",
        f"- Temperature: `{summary.get('temperature')}`",
        "",
        "## Checks",
    ]
    for item in summary.get("checks", []):
        lines.append(f"- `{item.get('name')}`: `{item.get('status')}` - {item.get('detail')}")
    if summary.get("error"):
        lines.extend(
            [
                "",
                "## Error",
                f"- Type: `{summary['error'].get('type')}`",
                f"- Category: `{summary['error'].get('category')}`",
                f"- Message: `{summary['error'].get('message')}`",
            ]
        )
    if summary.get("troubleshooting_hints"):
        lines.extend(["", "## Troubleshooting"])
        for hint in summary["troubleshooting_hints"]:
            lines.append(f"- {hint}")
    if summary.get("debug_path"):
        lines.extend(["", "## Artifacts", f"- Debug JSON: `{summary['debug_path']}`"])
    return "\n".join(lines).rstrip() + "\n"


def resolve_provider_config(
    provider_config_path: Path | None,
    provider_base_url: str | None,
    provider_api_key: str | None,
    provider_api_key_env: str,
    provider_model: str | None,
    provider_name: str | None,
    token_limit: int | None,
    temperature: float | None,
    timeout_seconds: float | None,
) -> OpenAICompatibleConfig:
    payload: dict[str, Any] = {}
    if provider_config_path:
        payload = json.loads(provider_config_path.read_text(encoding="utf-8"))

    base_url = str(provider_base_url or payload.get("base_url") or "").strip()
    model = str(provider_model or payload.get("model") or "").strip()
    if not base_url:
        raise LlmProviderError("Missing provider base URL. Use --provider-base-url or --provider-config.")
    if not model:
        raise LlmProviderError("Missing provider model. Use --provider-model or --provider-config.")

    api_key_env = str(payload.get("api_key_env") or provider_api_key_env or "OPENAI_API_KEY")
    api_key = str(provider_api_key or payload.get("api_key") or os.environ.get(api_key_env, "")).strip()
    if not api_key:
        raise LlmProviderError(
            f"Missing provider API key. Use --provider-api-key, set {api_key_env}, or provide api_key in --provider-config."
        )

    resolved_token_limit = int(token_limit if token_limit is not None else payload.get("token_limit", 2048))
    if resolved_token_limit <= 0:
        raise LlmProviderError("token limit must be a positive integer.")

    resolved_temperature = float(temperature if temperature is not None else payload.get("temperature", 0.0))
    resolved_timeout = float(timeout_seconds if timeout_seconds is not None else payload.get("timeout_seconds", 60.0))
    if resolved_timeout <= 0:
        raise LlmProviderError("timeout_seconds must be greater than zero.")

    headers = payload.get("headers", {})
    if not isinstance(headers, dict):
        raise LlmProviderError("provider config field 'headers' must be an object if provided.")

    return OpenAICompatibleConfig(
        base_url=base_url,
        model=model,
        api_key=api_key,
        token_limit=resolved_token_limit,
        temperature=resolved_temperature,
        timeout_seconds=resolved_timeout,
        provider_name=str(provider_name or payload.get("provider_name") or payload.get("name") or "").strip() or None,
        api_key_env=api_key_env,
        headers={str(key): str(value) for key, value in headers.items()},
        system_prompt=str(payload.get("system_prompt") or DEFAULT_SYSTEM_PROMPT),
    )


def post_chat_completion(config: OpenAICompatibleConfig, prompt_text: str) -> dict[str, Any]:
    url = normalize_chat_completions_url(config.base_url)
    payload = {
        "model": config.model,
        "messages": [
            {"role": "system", "content": config.system_prompt},
            {"role": "user", "content": prompt_text},
        ],
        "temperature": config.temperature,
        "max_tokens": config.token_limit,
        "stream": False,
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {config.api_key}",
    }
    headers.update(config.headers)
    return _post_json(
        url=url,
        payload=payload,
        headers=headers,
        timeout_seconds=config.timeout_seconds,
    )


def _post_json(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(url=url, data=body, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8")
            content_type = str(resp.headers.get("Content-Type") or "")
    except error.HTTPError as exc:
        error_body = ""
        if exc.fp is not None:
            error_body = exc.fp.read().decode("utf-8", errors="replace")
        raise LlmProviderError(build_http_error_message(url, exc.code, error_body)) from exc
    except error.URLError as exc:
        raise LlmProviderError(
            f"Failed to reach LLM provider at {url}: {exc.reason}. Check network access and provider base URL."
        ) from exc

    try:
        if content_type.lower().startswith("text/event-stream") or raw.lstrip().startswith("data:"):
            payload = parse_sse_chat_completion(raw)
        else:
            payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LlmProviderError(
            build_non_json_error_message(url, raw, content_type, exc.msg)
        ) from exc
    return payload


def extract_response_text(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise LlmProviderError("LLM response is missing choices[0].message.content.")
    message = choices[0].get("message", {})
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
        if parts:
            return "".join(parts)
    raise LlmProviderError("LLM response message.content is missing or not a supported text shape.")


def normalize_chat_completions_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    if normalized.endswith("/v1"):
        return f"{normalized}/chat/completions"
    return f"{normalized}/v1/chat/completions"


def build_http_error_message(url: str, status_code: int, error_body: str) -> str:
    detail = error_body.strip()[:1200] if error_body else "no response body"
    if status_code in {401, 403}:
        hint = "Check the API key and whether the provider accepts Bearer authentication."
    elif status_code == 404:
        hint = "Check the provider base URL. For OpenAI-compatible providers you usually want the /v1 root."
    elif status_code == 429:
        hint = "The provider rate-limited the request. Reduce traffic or use a different model/provider quota."
    elif status_code >= 500:
        hint = "The provider failed internally. Retry later or inspect provider logs."
    else:
        hint = "Inspect the provider response body for compatibility details."
    return f"LLM provider request failed with HTTP {status_code} at {url}. {hint}\nProvider body: {detail}"


def build_non_json_error_message(url: str, raw_body: str, content_type: str, decode_error: str) -> str:
    preview = raw_body.strip()[:1200] if raw_body else "empty response body"
    lowered = raw_body.lower()
    normalized_content_type = content_type or "unknown"
    if "text/html" in normalized_content_type.lower() or "<html" in lowered or "<!doctype html" in lowered:
        hint = "The provider or an upstream gateway returned HTML. Check the base URL, auth gateway, proxy, or SSO interstitial."
    elif normalized_content_type.lower().startswith("text/event-stream") or lowered.startswith("data:") or "\ndata:" in lowered:
        hint = "The provider looks like it returned SSE/streaming content. Use a non-streaming OpenAI-compatible chat/completions endpoint."
    else:
        hint = "The provider returned a vendor-specific plaintext payload or gateway message. Validate the endpoint with `validate-provider` and inspect the response preview."
    return (
        f"LLM provider returned non-JSON content from {url}: {decode_error}. "
        f"Content-Type: {normalized_content_type}. {hint}\n"
        f"Response preview: {preview}"
    )


def parse_sse_chat_completion(raw_body: str) -> dict[str, Any]:
    chunks: list[dict[str, Any]] = []
    for raw_line in raw_body.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if not data or data == "[DONE]":
            continue
        payload = json.loads(data)
        if isinstance(payload, dict):
            chunks.append(payload)
    if not chunks:
        raise json.JSONDecodeError("No JSON events found in SSE stream.", raw_body, 0)

    content_by_index: dict[int, list[str]] = {}
    role_by_index: dict[int, str] = {}
    finish_reason_by_index: dict[int, Any] = {}
    usage: dict[str, Any] = {}
    response_id = None
    created = None
    model = None
    for chunk in chunks:
        if response_id is None:
            response_id = chunk.get("id")
        if created is None:
            created = chunk.get("created")
        if model is None:
            model = chunk.get("model")
        if isinstance(chunk.get("usage"), dict):
            usage = chunk["usage"]
        for choice in chunk.get("choices", []) if isinstance(chunk.get("choices"), list) else []:
            if not isinstance(choice, dict):
                continue
            index = int(choice.get("index", 0))
            delta = choice.get("delta")
            message = choice.get("message")
            role = None
            content_value: Any = None
            if isinstance(delta, dict):
                role = delta.get("role")
                content_value = delta.get("content")
            elif isinstance(message, dict):
                role = message.get("role")
                content_value = message.get("content")
            if isinstance(role, str) and role:
                role_by_index[index] = role
            if isinstance(content_value, str):
                content_by_index.setdefault(index, []).append(content_value)
            elif isinstance(content_value, list):
                text_parts: list[str] = []
                for item in content_value:
                    if isinstance(item, str):
                        text_parts.append(item)
                    elif isinstance(item, dict) and item.get("type") == "text":
                        text_parts.append(str(item.get("text", "")))
                if text_parts:
                    content_by_index.setdefault(index, []).append("".join(text_parts))
            finish_reason_by_index[index] = choice.get("finish_reason")

    all_indexes = content_by_index.keys() | role_by_index.keys() | finish_reason_by_index.keys()
    if not all_indexes:
        raise json.JSONDecodeError("No usable choices found in SSE stream.", raw_body, 0)
    max_index = max(all_indexes)
    choices: list[dict[str, Any]] = []
    for index in range(max_index + 1):
        choices.append(
            {
                "index": index,
                "message": {
                    "role": role_by_index.get(index, "assistant"),
                    "content": "".join(content_by_index.get(index, [])),
                },
                "finish_reason": finish_reason_by_index.get(index),
            }
        )
    return {
        "id": response_id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": choices,
        "usage": usage,
        "stream_reconstructed": True,
    }


def build_request_artifact(config: OpenAICompatibleConfig, prompt_text: str) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "provider_name": config.provider_name,
        "url": normalize_chat_completions_url(config.base_url),
        "model": config.model,
        "token_limit": config.token_limit,
        "temperature": config.temperature,
        "timeout_seconds": config.timeout_seconds,
        "api_key_env": config.api_key_env,
        "headers": {"Content-Type": "application/json", "Accept": "application/json", **config.headers},
        "request_payload": {
            "model": config.model,
            "messages": [
                {"role": "system", "content": config.system_prompt},
                {"role": "user", "content": prompt_text},
            ],
            "temperature": config.temperature,
            "max_tokens": config.token_limit,
            "stream": False,
        },
    }


def append_run_summary_to_index(run_root: Path, summary_path: Path, run_summary: dict[str, Any]) -> None:
    index_path = run_root / "index.json"
    payload = {"runs": []}
    if index_path.exists():
        try:
            loaded = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
        except json.JSONDecodeError:
            payload = {"runs": []}

    payload.setdefault("runs", [])
    indexed_summary = dict(run_summary)
    indexed_summary["summary_path"] = str(summary_path)
    runs = [item for item in payload["runs"] if isinstance(item, dict)]
    runs = [item for item in runs if item.get("summary_path") != indexed_summary["summary_path"]]
    runs.append(indexed_summary)
    runs.sort(key=lambda item: str(item.get("generated_at") or ""))
    payload["runs"] = runs
    index_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def render_run_summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# LLM Run Summary",
        "",
        "## Request",
        f"- Cluster: `{summary['cluster_id']}`",
        f"- Stage: `{summary['stage']}`",
        f"- Prompt budget: `{summary['budget']}`",
        f"- Prompt model label: `{summary['prompt_model']}`",
        f"- Provider: `{summary['provider_name']}`",
        f"- Provider model: `{summary['provider_model']}`",
        f"- Token limit: `{summary['token_limit']}`",
        f"- Temperature: `{summary['temperature']}`",
        "",
        "## Prompt",
        f"- Prompt path: `{summary['prompt_path']}`",
        f"- Prompt SHA256: `{summary['prompt_sha256']}`",
        f"- Estimated tokens: `{summary['prompt_estimated_tokens']}`",
        "",
        "## Response",
        f"- Response id: `{summary.get('response_id') or 'n/a'}`",
        f"- Usage: `{json.dumps(summary.get('response_usage', {}), ensure_ascii=False)}`",
    ]
    if summary.get("review_enabled"):
        lines.extend(
            [
                "",
                "## Review",
                f"- Enabled: `{summary['review_enabled']}`",
                f"- Review status: `{summary.get('review_status', 'n/a')}`",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"
