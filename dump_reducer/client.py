from typing import Any, Dict, List, TypedDict, Optional

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
    def __init__(self, api_key: str, model: str, verify: bool = True) -> None:
        self.api_key = api_key
        self.model = model
        self.verify = verify

    def chat(self, messages: List[Message], tools: List[Dict[str, Any]]) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-Title": "db-subset-agent",
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.2,
        }
        try:
            resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120, verify=self.verify)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.SSLError as e:
            raise RuntimeError(
                f"SSL verification to OpenRouter API. "
                f"Try running with --no-verify-ssl if you trust the network.\nOriginal error: {e}"
            ) from e
