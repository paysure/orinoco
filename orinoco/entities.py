import abc
from functools import partial
from typing import Dict, Any, Optional, Set, Type, List, Tuple, Sequence, ClassVar, Generic, Awaitable, Union

from pydantic import Field

from orinoco.exceptions import SearchError, NothingFound, FoundMoreThanOne, AlreadyRegistered
from orinoco.helpers import initialize
from orinoco.observers import ExecutionTimeObserver, ActionsLog
from orinoco.types import (
    SignatureT,
    BaseModelT,
    T,
    ActionConfigT,
    ActionDataT,
    ActionT,
    ObserverT,
    ObserverVar,
    ImmutableEvolvableModelT,
)


class ImmutableEvolvableModel(ImmutableEvolvableModelT, abc.ABC):
    def evolve_self(self: BaseModelT, **kwargs: Any) -> BaseModelT:
        return self.copy(update=kwargs)


class Signature(Generic[T], ImmutableEvolvableModel, SignatureT[T]):
    type_: Optional[Type[T]] = None
    tags: Set[str] = Field(default_factory=set)
    key: Optional[str] = None

    def match(self, other_signature: "SignatureT[T]") -> bool:
        """
        Match with another signature. Note that matching with for ``tags`` set is done as "is subset"
        """
        if (
            other_signature.type_ is not None
            and self.type_ is not None
            and not issubclass(self.type_, other_signature.type_)
        ):
            return False

        if len(other_signature.tags) > 0 and not other_signature.tags.issubset(self.tags):
            return False

        if other_signature.key is not None and other_signature.key != self.key:
            return False
        return True

    def __class_getitem__(cls: T, _: Any) -> T:
        # Fix for pydantic to support generic types (expressions like `SignatureT[bool]`)
        return cls


class ActionConfig(Generic[T], ImmutableEvolvableModel, ActionConfigT[T]):
    INPUT: Optional[Dict[str, SignatureT[Any]]] = None
    OUTPUT: Optional[SignatureT[T]] = None

    @classmethod
    def create_strict(
        cls,
        input_: Optional[Dict[str, Type[Any]]] = None,
        output_type: Optional[Type[T]] = None,
        output_name: Optional[str] = None,
    ) -> "ActionConfig[T]":
        return cls(
            INPUT=({name: Signature(type_=type_, key=name) for name, type_ in input_.items()}) if input_ else None,
            OUTPUT=Signature(type_=output_type, key=output_name),
        )

    def __class_getitem__(cls: T, _: Any) -> T:
        # Fix for pydantic to support generic types (expressions like `SignatureT[bool]`)
        return cls


class ActionData(ImmutableEvolvableModel, ActionDataT):
    """
    Fundamental data structure which is both input and output of actions.

    Virtual keys: To save a memory some data are not stored directly, but just a method how to get them from other
    data stored in `ActionData`.

    `ActionData.data`, `ActionData.virtual_keys` and `ActionData` are immutable data structures, so usually a copy
    is made after every action it pass thru with the new data.

    History of all actions done on the dataset are recorded in `ActionData.actions_done`.
    """

    NOT_FOUND: ClassVar = object()
    DEFAULT_OBSERVERS_CLASSES: ClassVar = (ExecutionTimeObserver, ActionsLog)

    data: Tuple[Tuple[SignatureT[Any], Any], ...] = Field(default_factory=tuple)
    futures: List[Awaitable[Any]] = Field(default_factory=list)
    observers: List[ObserverT] = Field(default_factory=partial(initialize, DEFAULT_OBSERVERS_CLASSES))

    skip_processing: bool = False

    # Shortcuts
    def __getitem__(self, searched_signature: SignatureT[T]) -> T:
        return self.get_by_signature(searched_signature)

    def get(self, key: str, default: Any = NOT_FOUND) -> Any:
        """
        Get item by ``key``
        """
        return self.get_by_key(key, default=default)

    # Getters
    def get_by_signature(self, searched_signature: SignatureT[T]) -> T:
        """
        Get data with the given signature -- all non-default fields of the signature will be used for matching
        """
        return self.get_with_signature(searched_signature=searched_signature)[1]

    def get_with_signature(self, searched_signature: SignatureT[T]) -> Tuple[SignatureT[T], T]:
        """
        Get data + signature with the given signature -- all non-default fields of the signature will be used
        for matching
        """
        try:
            return self._ensure_one(
                [(signature, entity) for signature, entity in self.data if signature == searched_signature]
            )
        except SearchError as err:
            raise SearchError(
                "Failed to find {}\nPresent signatures: {}".format(searched_signature, self.signatures)
            ) from err

    def get_or_default(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Get data by key or return a default value
        """
        try:
            return self.get_by_signature(Signature(key=key))
        except (NothingFound, SearchError):
            return default

    def find_or_default(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Find a single value with matching or default
        """
        try:
            return self.find_one(Signature(key=key))
        except NothingFound:
            return default

    def get_by_key(self, key: str, default: Any = NOT_FOUND) -> Any:
        """
        Get item by ``key``
        """
        try:
            return self.find_one(Signature(key=key))
        except NothingFound:
            if "." in key:
                name_parts = key.split(".")
                result = self._get_from_nested(
                    key=".".join(name_parts[1:]),
                    data=self.find_one(Signature(key=name_parts[0])),
                    default=self.NOT_FOUND,
                )
                if result != self.NOT_FOUND:
                    return result

            if default != self.NOT_FOUND:
                return default
            raise

    def get_by_type(self, type_: Type[T]) -> T:
        """
        Get a value by matching type.

        :raises
         - `NothingFound`: When no data match the signature
         - `FoundMoreThanOne`: When multiple data match the signature
        """
        return self.find_one(Signature(type_=type_))

    def get_by_tags(self, *tags: str) -> Any:
        """
        Get a value by matching tags.

        :raises
         - `NothingFound`: When no data match the signature
         - `FoundMoreThanOne`: When multiple data match the signature
        """
        return self.find_one(Signature(tags=set(tags)))

    # Searching
    def find(self, searched_signature: SignatureT[T]) -> List[T]:
        """
        Find values which match the signature
        """
        return list(map(lambda signature_value: signature_value[1], self.find_with_signature(searched_signature)))

    def find_one(self, searched_signature: SignatureT[T]) -> T:
        """
        Find single value which match the signature

        :raises
         - `NothingFound`: When no data match the signature
         - `FoundMoreThanOne`: When multiple data match the signature
        """
        try:
            return self._ensure_one(self.find(searched_signature=searched_signature))
        except SearchError as err:
            raise err.__class__(
                "Failed to find {}\nPresent signatures: {}".format(searched_signature, self.signatures)
            ) from err

    def find_with_signature(self, searched_signature: SignatureT[T]) -> List[Tuple[SignatureT[T], T]]:
        """
        Find values with signatures which match the signature
        """
        return [(signature, entity) for signature, entity in self.data if signature.match(searched_signature)]

    # --- Evolution
    def evolve(self, **data: Any) -> "ActionData":
        """
        Add values to the data with signatures created from keys (parameter names)

        Already registered data with signatures with keys matching the new will be removed and replaced by the new ones
        """
        # TODO: Can be optimized
        existing = {}
        for key in data.keys():
            try:
                signature: SignatureT[Any] = self._ensure_one(self.find_with_signature(Signature(key=key)))[0]
                existing[key] = signature

            except SearchError:
                pass

        return self.remove_many(searched_signatures=list(existing.values()), ignore_non_existent=True).register_many(
            [(existing.get(key, Signature(key=key, type_=type(value))), value) for key, value in data.items()]
        )

    def register(self, signature: SignatureT[T], entity: T, check_if_exists: bool = True) -> "ActionData":
        """
        Add new value with the given signature
        :param signature: Signature of the entity
        :param entity: Entity which will be added
        :param check_if_exists: Controls whether the method raises an exception if a value with
        the ``signature`` exists
        :return: Copy of itself
        """
        if check_if_exists:
            try:
                self.get_by_signature(searched_signature=signature)
                raise AlreadyRegistered("Entity with signature {} is already registered".format(signature))
            except SearchError:
                pass

        # Remove data with an existing key
        instance = self
        if signature.key:
            instance = self.remove(
                searched_signature=Signature(key=signature.key), ignore_non_existent=True, exact_match=False
            )
        return instance.evolve_self(data=instance.data + ((signature, entity),))

    def register_many(self, data: Sequence[Tuple[SignatureT[T], T]], check_if_exists: bool = True) -> "ActionData":
        """
        Add new values with the given signatures
        :param data: Items with signatures to be added
        :param check_if_exists: Controls whether the method raises an exception if a value with
        the ``signature`` exists

        :return: Copy of itself
        """
        instance = self
        for signature, entity in data:
            instance = self.register(signature=signature, entity=entity, check_if_exists=check_if_exists)
        return instance

    def remove(
        self, searched_signature: SignatureT[Any], ignore_non_existent: bool = False, exact_match: bool = True
    ) -> "ActionData":
        """
        Remove a value matching the signature
        :param searched_signature: Looked up signature
        :param ignore_non_existent: Controls whether the method raises an exception if a value with
        the ``signature`` wasn't found
        :param exact_match: Controls whether exact signature should be found
        :return:
        """
        try:
            if exact_match:
                to_remove = self.get_with_signature(searched_signature=searched_signature)[0]
            else:
                to_remove = self.find_with_signature(searched_signature=searched_signature)[0][0]
        except (NothingFound, IndexError):
            if not ignore_non_existent:
                raise
            return self.evolve_self()

        return self.evolve_self(data=tuple(filter(lambda signature_value: signature_value[0] != to_remove, self.data)))

    def remove_many(
        self,
        searched_signatures: Sequence[SignatureT[Any]],
        ignore_non_existent: bool = False,
        exact_match: bool = True,
    ) -> "ActionData":
        """
        Remove values matching the signatures
        :param searched_signatures: Looked up signatures
        :param ignore_non_existent: Controls whether the method raises an exception if a value with
        the ``signature`` wasn't found
        :param exact_match: Controls whether exact signature should be found
        :return: Copy of self
        """

        instance = self
        for searched_signature in searched_signatures:
            instance = instance.remove(
                searched_signature, ignore_non_existent=ignore_non_existent, exact_match=exact_match
            )
        return instance

    def with_new_execution_meta(self) -> "ActionData":
        """
        Clean all metadata from the previous execution. Note that observers will be created from the default classes

        :return: Copy of self with only :attrs:`ActionData.data` preserved
        """
        return ActionData(data=self.data)

    def with_future(self, future: Awaitable[Any]) -> "ActionData":
        """
        Log futures references
        """
        return self.evolve_self(futures=self.futures + [future])

    def record_start(self, action: ActionT) -> "ActionData":
        """
        :param action: Action which is recorded
        :return: New copy with recorded start of the action
        """
        for observer in self.observers:
            if observer.should_record_action(action):
                observer.record_start(action)
        return self

    def record_end(self, action: ActionT) -> "ActionData":
        """
        :param action: Action which is recorded
        :return: New copy with recorded end of the action
        """
        for observer in self.observers:
            if observer.should_record_action(action):
                observer.record_end(action)
        return self

    @classmethod
    def create(cls, **data: Any) -> "ActionData":
        """
        Convenient way how to create the :class:`ActionData` with only key signatures

        :param data: Data values of keys-values
        :return: Created :class:`ActionData`
        """
        return ActionData(data=cls._keyed_dict_as_values_with_signatures(data))

    # Export
    def as_keyed_dict(self) -> Dict[str, Any]:
        """
        Export self into a dictionary for keys-values for signatures which have keys set
        """
        return {signature.key: value for signature, value in self.data if signature.key}

    def with_new_data(self, data: Dict[str, Any]) -> "ActionData":
        """
        :param data: New data to be appended
        :return: Copy with new data
        """
        return self.evolve_self(data=self._keyed_dict_as_values_with_signatures(data))

    @property
    def signatures(self) -> List[SignatureT[Any]]:
        """
        List registered signatures
        """
        return [reg[0] for reg in self.data]

    def is_in(self, key: str) -> bool:
        """
        Find if the key is in the data
        """
        try:
            self.get_by_key(key)
            return True
        except NothingFound:
            return False

    def signature_is_in(self, searched_signature: SignatureT[Any]) -> bool:
        """
        Find if the signature is in the data
        """
        try:
            self.get_by_signature(searched_signature)
            return True
        except NothingFound:
            return False

    def get_observer(self, observer_cls: Type[ObserverVar]) -> ObserverVar:
        """
        :param observer_cls: Class of the observer to find
        :return: Observer from the observers attached to this container
        """
        return self._ensure_one([observer for observer in self.observers if isinstance(observer, observer_cls)])

    @classmethod
    def _get_from_nested(cls, key: str, data: Dict[str, Any], default: Optional[Any] = None) -> Any:
        first_dot_index = key.find(".")
        if first_dot_index > 0:
            if key[:first_dot_index] not in data:
                return default

            return cls._get_from_nested(key[first_dot_index + 1 :], data[key[:first_dot_index]])
        return data.get(key, default)

    @staticmethod
    def _ensure_one(matched: Sequence[T]) -> T:
        n_matched = len(matched)

        if n_matched == 1:
            return matched[0]
        elif n_matched == 0:
            raise NothingFound()

        # Check for duplicates
        first_value = matched[0]
        for value in matched[1:]:
            if value != first_value:
                raise FoundMoreThanOne("Expected one, but found {}".format(n_matched))
        return first_value

    @staticmethod
    def _keyed_dict_as_values_with_signatures(data: Dict[str, Any]) -> Tuple[Tuple[SignatureT[Any], Any], ...]:
        return tuple((Signature(key=key, type_=type(value)), value) for key, value in data.items())
