from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar, Optional, Type, List, Tuple, Sequence, ClassVar, Set, Generic

from pydantic import BaseModel


T = TypeVar("T")
BaseModelT = TypeVar("BaseModelT", bound=BaseModel)


class NamespacedActionT:
    class_to_return: Type["ActionT"]

    @abstractmethod
    def __getattr__(self, _: Any) -> Type["ActionT"]:
        pass


class ImmutableEvolvableModelT(BaseModel, ABC):
    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True

    @abstractmethod
    def evolve_self(self: T, **kwargs: Any) -> T:
        pass


class SignatureT(Generic[T], ImmutableEvolvableModelT, ABC):
    type_: Optional[Type[T]]
    tags: Set[str]
    key: Optional[str]

    @abstractmethod
    def match(self, other_signature: "SignatureT[T]") -> bool:
        pass


class ActionConfigT(Generic[T], ImmutableEvolvableModelT, ABC):
    INPUT: Optional[Dict[str, SignatureT[Any]]]
    OUTPUT: Optional[SignatureT[T]]


class ObserverT(ABC):
    @abstractmethod
    def should_record_action(self, action: "ActionT") -> bool:
        pass

    @abstractmethod
    def record_start(self, action: "ActionT") -> None:
        pass

    @abstractmethod
    def record_end(self, action: "ActionT") -> None:
        pass


class ActionDataT(ImmutableEvolvableModelT, ABC):

    NOT_FOUND: ClassVar = object()

    # data: List[Tuple[SignatureT[Any], Any]]
    data: Tuple[Tuple[SignatureT[Any], Any], ...]
    futures: List[Any]
    observers: List[ObserverT]
    skip_processing: bool

    @abstractmethod
    # @property
    def signatures(self) -> List[SignatureT[Any]]:
        pass

    @abstractmethod
    def __getitem__(self, searched_signature: SignatureT[T]) -> T:
        pass

    @abstractmethod
    def get(self, name: str, default: Any = NOT_FOUND) -> Any:
        pass

    @abstractmethod
    def get_by_signature(self, searched_signature: SignatureT[T]) -> T:
        pass

    @abstractmethod
    def get_with_signature(self, searched_signature: SignatureT[T]) -> Tuple[SignatureT[T], T]:
        pass

    @abstractmethod
    def get_or_default(self, key: str, default: Optional[Any] = None) -> Any:
        pass

    @abstractmethod
    def find_or_default(self, key: str, default: Optional[Any] = None) -> Any:
        pass

    @abstractmethod
    def get_by_key(self, name: str, default: Any = NOT_FOUND) -> Any:
        pass

    @abstractmethod
    def get_by_type(self, type_: Type[T]) -> T:
        pass

    @abstractmethod
    def get_by_tags(self, *tags: str) -> Any:
        pass

    @abstractmethod
    def find(self, searched_signature: SignatureT[T]) -> List[T]:
        pass

    @abstractmethod
    def find_one(self, searched_signature: SignatureT[T]) -> T:
        pass

    @abstractmethod
    def find_with_signature(self, searched_signature: SignatureT[T]) -> List[Tuple[SignatureT[T], T]]:
        pass

    @abstractmethod
    def evolve(self, **data: Any) -> "ActionDataT":
        pass

    @abstractmethod
    def register(self, signature: SignatureT[T], entity: T, check_if_exists: bool = True) -> "ActionDataT":
        pass

    @abstractmethod
    def register_many(self, data: Sequence[Tuple[SignatureT[T], T]], check_if_exists: bool = True) -> "ActionDataT":
        pass

    @abstractmethod
    def remove(
        self, searched_signature: SignatureT[Any], ignore_non_existent: bool = False, exact_match: bool = True
    ) -> "ActionDataT":
        pass

    @abstractmethod
    def remove_many(
        self,
        searched_signatures: Sequence[SignatureT[Any]],
        ignore_non_existent: bool = False,
        exact_match: bool = True,
    ) -> "ActionDataT":
        pass

    @abstractmethod
    def with_future(self, future: Any) -> "ActionDataT":
        pass

    @abstractmethod
    def record_start(self, action: "ActionT") -> "ActionDataT":
        pass

    @abstractmethod
    def record_end(self, action: "ActionT") -> "ActionDataT":
        pass

    @classmethod
    @abstractmethod
    def create(cls, **data: Any) -> "ActionDataT":
        pass

    @abstractmethod
    def as_keyed_dict(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def with_new_data(self, data: Dict[str, Any]) -> "ActionDataT":
        pass

    @abstractmethod
    def is_in(self, key: str) -> bool:
        pass

    @abstractmethod
    def signature_is_in(self, searched_signature: SignatureT[Any]) -> bool:
        pass

    @abstractmethod
    def get_observer(self, observer_cls: Type["ObserverVar"]) -> "ObserverVar":
        pass

    @abstractmethod
    def with_new_execution_meta(self) -> "ActionDataT":
        pass


class ActionT(ABC):

    DESCRIPTION: str
    NAME: str

    description: str
    name: str

    # @abstractmethod
    @property
    def action_name(self) -> str:
        pass

    @abstractmethod
    def then(self, another_action: "ActionT") -> "ActionT":
        pass

    @abstractmethod
    def __rshift__(self, other: "ActionT") -> "ActionT":
        pass

    @abstractmethod
    def on_subfield(self, field_key: str) -> "ActionT":
        pass

    @classmethod
    @abstractmethod
    def namespaced(cls) -> NamespacedActionT:
        pass

    @abstractmethod
    def run(self, action_data: ActionDataT) -> ActionDataT:
        pass

    @abstractmethod
    def run_with_data(self, **params: Any) -> ActionDataT:
        pass

    @abstractmethod
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        pass

    @abstractmethod
    async def async_run_with_data(self, **params: Any) -> ActionDataT:
        pass

    @abstractmethod
    def finish(self) -> "ActionT":
        pass


ActionVar = TypeVar("ActionVar", bound=ActionT)
ObserverVar = TypeVar("ObserverVar", bound=ObserverT)

TypeT = TypeVar("TypeT")
AnnotationNameT = TypeVar("AnnotationNameT", bound=str)


ErrorT = TypeVar("ErrorT", bound=BaseException)
