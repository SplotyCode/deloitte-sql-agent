import pytest

from dump_reducer.planner import _extract_plan_json


def test_extract_plan_json_accepts_strict_json():
    plan = _extract_plan_json('{"tables":["users"],"steps":[]}')
    assert plan == {"tables": ["users"], "steps": []}


def test_extract_plan_json_accepts_leading_text_before_fenced_json():
    plan = _extract_plan_json(
        """
        Here is the final plan.

        ```json
        {
          "tables": ["users"],
          "steps": []
        }
        ```
        """
    )
    assert plan == {"tables": ["users"], "steps": []}


def test_extract_plan_json_rejects_missing_json():
    with pytest.raises(Exception):
        _extract_plan_json("No plan here.")
