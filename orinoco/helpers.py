import string
from typing import Iterable, Type, List, Any, NoReturn, Union, Optional, Tuple, Set

from typing_extensions import Annotated, get_origin, get_args

from orinoco.types import TypeT, AnnotationNameT


def initialize(classes: Iterable[Type[Any]]) -> List[Any]:
    return [cls_object() for cls_object in classes]


def raise_not_provided_field(field_name: str) -> NoReturn:
    raise ValueError("Field {} has to be provided".format(field_name))


def is_format_string(value: str) -> bool:
    return any(get_format_string_args(value))


def get_format_string_args(value: str) -> List[str]:
    return [tup[1] for tup in string.Formatter().parse(value) if (tup[1] is not None and tup[1] != "")]


def extract_type(
    value: Union[TypeT, Annotated[TypeT, AnnotationNameT]]
) -> Tuple[TypeT, Optional[AnnotationNameT], Set[str]]:
    generic = get_origin(value)
    if generic is Annotated:
        dtype, annotation, *tags = get_args(value)  # first two parameters - (type, annotation)
        return dtype, annotation, set(tags)
    return value, None, set()
