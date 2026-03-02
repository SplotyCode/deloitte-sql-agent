import hashlib
import json
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

class OpenRouterClient:
    def __init__(self, api_key: str, model: str, verify: bool = True, cache_dir: Optional[str] = ".cache/openrouter") -> None:
        self.api_key = api_key
        self.model = model
        self.verify = verify
        self.cache_dir = Path(cache_dir) if cache_dir else None

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
            response = cached["response"]
            response["_cache"] = {"hit": True, "key": cache_key, "path": str(cache_path)}
            return response

        try:
            resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120, verify=self.verify)
            resp.raise_for_status()
            response = resp.json()
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
