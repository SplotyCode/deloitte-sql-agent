import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

import requests


class Message(TypedDict, total=False):
    role: str
    content: Optional[str]
    name: Optional[str]
    reasoning: Optional[str]
    tool_calls: Optional[List[Dict[str, Any]]]
    tool_call_id: Optional[str]

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


@dataclass
class OpenRouterStats:
    calls_total: int = 0
    network_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    billed_prompt_tokens: int = 0
    billed_completion_tokens: int = 0
    billed_total_tokens: int = 0
    cached_prompt_tokens: int = 0
    cached_completion_tokens: int = 0
    cached_total_tokens: int = 0
    billed_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0


class OpenRouterClient:
    def __init__(self, api_key: str, model: str, verify: bool = True, cache_dir: Optional[str] = ".cache/openrouter") -> None:
        self.api_key = api_key
        self.model = model
        self.verify = verify
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self._stats = OpenRouterStats()

    def _build_payload(self, messages: List[Message], tools: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.2,
        }

    def _cache_key(self, payload: Dict[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _cache_path(self, key: str) -> Optional[Path]:
        if self.cache_dir is None:
            return None
        return self.cache_dir / f"{key}.json"

    def _extract_usage(self, response: Dict[str, Any]) -> Dict[str, float]:
        usage = response.get("usage") or {}

        prompt_tokens = usage.get("prompt_tokens", usage.get("input_tokens", 0)) or 0
        completion_tokens = usage.get("completion_tokens", usage.get("output_tokens", 0)) or 0
        total_tokens = usage.get("total_tokens") or (prompt_tokens + completion_tokens)

        raw_cost = (
            usage.get("cost")
            or usage.get("total_cost")
            or usage.get("cost_usd")
            or response.get("cost")
            or 0.0
        )

        try:
            cost_usd = float(raw_cost)
        except (TypeError, ValueError):
            cost_usd = 0.0

        return {
            "prompt_tokens": int(prompt_tokens),
            "completion_tokens": int(completion_tokens),
            "total_tokens": int(total_tokens),
            "cost_usd": cost_usd,
        }

    def _record_usage(self, response: Dict[str, Any], cached: bool) -> None:
        usage = self._extract_usage(response)
        self._stats.calls_total += 1

        if cached:
            self._stats.cache_hits += 1
            self._stats.cached_prompt_tokens += int(usage["prompt_tokens"])
            self._stats.cached_completion_tokens += int(usage["completion_tokens"])
            self._stats.cached_total_tokens += int(usage["total_tokens"])
            self._stats.cached_cost_usd += float(usage["cost_usd"])
        else:
            self._stats.network_requests += 1
            self._stats.cache_misses += 1
            self._stats.billed_prompt_tokens += int(usage["prompt_tokens"])
            self._stats.billed_completion_tokens += int(usage["completion_tokens"])
            self._stats.billed_total_tokens += int(usage["total_tokens"])
            self._stats.billed_cost_usd += float(usage["cost_usd"])

    def get_stats(self) -> Dict[str, Any]:
        stats = asdict(self._stats)
        logical_prompt_tokens = self._stats.billed_prompt_tokens + self._stats.cached_prompt_tokens
        logical_completion_tokens = self._stats.billed_completion_tokens + self._stats.cached_completion_tokens
        logical_total_tokens = self._stats.billed_total_tokens + self._stats.cached_total_tokens
        logical_cost_usd = self._stats.billed_cost_usd + self._stats.cached_cost_usd
        stats.update(
            {
                "cache_hit_rate": (self._stats.cache_hits / self._stats.calls_total) if self._stats.calls_total else 0.0,
                "logical_prompt_tokens": logical_prompt_tokens,
                "logical_completion_tokens": logical_completion_tokens,
                "logical_total_tokens": logical_total_tokens,
                "logical_cost_usd": logical_cost_usd,
            }
        )
        return stats

    def chat(self, messages: List[Message], tools: List[Dict[str, Any]]) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-Title": "db-subset-agent",
        }
        payload = self._build_payload(messages, tools)
        cache_key = self._cache_key(payload)
        cache_path = self._cache_path(cache_key)

        if cache_path and cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            response = deepcopy(cached["response"])
            self._record_usage(response, cached=True)
            response["_cache"] = {"hit": True, "key": cache_key, "path": str(cache_path)}
            return response

        try:
            resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=60 * 4, verify=self.verify)
            resp.raise_for_status()
            response = resp.json()
            self._record_usage(response, cached=False)
            if cache_path:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(
                    json.dumps({"payload": payload, "response": response}, sort_keys=True, indent=2),
                    encoding="utf-8",
                )
            response["_cache"] = {"hit": False, "key": cache_key, "path": str(cache_path) if cache_path else None}
            return response
        except requests.exceptions.SSLError as e:
            raise RuntimeError(
                f"SSL verification to OpenRouter API. "
                f"Try running with --no-verify-ssl if you trust the network.\nOriginal error: {e}"
            ) from e
