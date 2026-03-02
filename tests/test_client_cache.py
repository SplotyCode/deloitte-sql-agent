from unittest.mock import Mock, patch

from dump_reducer.client import OpenRouterClient


def test_openrouter_client_reuses_disk_cache(tmp_path):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": '{"steps":[]}',
                }
            }
        ]
    }

    messages = [{"role": "user", "content": "hello"}]
    tools = []

    with patch("dump_reducer.client.requests.post", return_value=response) as mocked_post:
        client = OpenRouterClient("key", "model", cache_dir=str(tmp_path))

        first = client.chat(messages, tools)
        second = client.chat(messages, tools)

    assert mocked_post.call_count == 1
    assert first["_cache"]["hit"] is False
    assert second["_cache"]["hit"] is True
    assert first["choices"][0]["message"]["content"] == second["choices"][0]["message"]["content"]
    assert len(list(tmp_path.glob("*.json"))) == 1
