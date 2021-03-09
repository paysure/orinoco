from typing import Iterable, Type, List, Any, NoReturn


def initialize(classes: Iterable[Type[Any]]) -> List[Any]:
    return [cls_object() for cls_object in classes]


def raise_not_provided_field(field_name: str) -> NoReturn:
    raise ValueError("Field {} has to be provided".format(field_name))
