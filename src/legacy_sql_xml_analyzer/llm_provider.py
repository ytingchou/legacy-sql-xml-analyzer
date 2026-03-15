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

    append_artifacts_to_index(analysis_root.parent, run_artifacts)
    return {
        "run_summary": run_summary,
        "response_text": response_text,
        "response_payload": response_payload,
        "artifacts": all_artifacts,
        "review": review_result,
    }


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
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LlmProviderError(
            f"LLM provider returned non-JSON content from {url}: {exc.msg}."
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
        },
    }


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
