from typing import Iterable, Type, List, Any, NoReturn, Union, Optional, Tuple

from typing_extensions import Annotated, TypeGuard

from orinoco.types import TypeT, AnnotationNameT


def initialize(classes: Iterable[Type[Any]]) -> List[Any]:
    return [cls_object() for cls_object in classes]


def raise_not_provided_field(field_name: str) -> NoReturn:
    raise ValueError("Field {} has to be provided".format(field_name))


def _extract_type(value: Union[TypeT, Annotated[TypeT, AnnotationNameT]]) -> Tuple[TypeT, Optional[AnnotationNameT]]:
    if _is_annotated(value):
        return value.__origin__, value.__metadata__[0]
    return value, None


def _is_annotated(
    value: Union[TypeT, Annotated[TypeT, AnnotationNameT]]
) -> TypeGuard[Annotated[TypeT, AnnotationNameT]]:
    # Annotated can't be used in `isinstance` or `issubclass` checks
    return value is not None and value.__name__ == "Annotated"
