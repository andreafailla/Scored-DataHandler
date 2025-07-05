from typing import Dict, Any, Callable
import re
from typing import List
from collections.abc import Callable


def create_text_filter(
    keywords: List[str], require_all: bool = False
) -> Callable[[str], bool]:
    """
    Create a text filter function based on keywords.

    Args:
        keywords: List of keywords to search for
        require_all: If True, all keywords must be present

    Returns:
        Filter function
    """

    def filter_func(text: str) -> bool:
        text_lower = text.lower()
        if require_all:
            return all(keyword.lower() in text_lower for keyword in keywords)
        else:
            return any(keyword.lower() in text_lower for keyword in keywords)

    return filter_func


def create_metadata_filter(
    conditions: Dict[str, Any] = None, **kwargs
) -> Callable[[Dict], bool]:
    """
    Create a metadata filter function based on conditions.

    Args:
        conditions: Dictionary of field conditions. Can use operators like:
                   - field__gt: greater than
                   - field__gte: greater than or equal
                   - field__lt: less than
                   - field__lte: less than or equal
                   - field__ne: not equal
                   - field__in: in list/set
                   - field__contains: substring/element contains
                   - field__regex: regex match
                   - field: exact match
        **kwargs: Alternative way to specify conditions

    Returns:
        Filter function
    """
    if conditions is None:
        conditions = kwargs

    def filter_func(metadata: Dict) -> bool:
        for field_condition, value in conditions.items():
            if "__" in field_condition:
                field, operator = field_condition.rsplit("__", 1)
            else:
                field, operator = field_condition, "eq"

            field_value = metadata.get(field)

            if operator == "gt":
                if not (field_value is not None and field_value > value):
                    return False
            elif operator == "gte":
                if not (field_value is not None and field_value >= value):
                    return False
            elif operator == "lt":
                if not (field_value is not None and field_value < value):
                    return False
            elif operator == "lte":
                if not (field_value is not None and field_value <= value):
                    return False
            elif operator == "ne":
                if not (field_value != value):
                    return False
            elif operator == "in":
                if not (field_value in value):
                    return False
            elif operator == "contains":
                if not (field_value is not None and value in field_value):
                    return False
            elif operator == "regex":
                if not (field_value is not None and re.search(value, str(field_value))):
                    return False
            elif operator == "eq":
                if not (field_value == value):
                    return False
            else:
                raise ValueError(f"Unknown operator: {operator}")

        return True

    return filter_func
