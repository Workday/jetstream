"""Dynamic ServiceNow table resolution."""
from __future__ import annotations

import pytest

from src.objects import resolve_object, select_objects


def test_empty_selection_uses_incident():
    objects = select_objects("")
    assert [obj.sn_table for obj in objects] == ["incident"]


def test_blank_name_is_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        resolve_object("  ")


def test_meta_table_defaults():
    obj = resolve_object("problem")
    assert obj.attachment_meta_table == "problem_attachments"
    assert obj.journals_meta_table == "problem_journals"
    assert obj.primary_key == "sys_id"
    assert obj.watermark_columns == ("sys_updated_on",)
