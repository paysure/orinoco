from abc import ABC
from typing import Optional, Dict, Any, Type, Callable, Generic, Awaitable

from orinoco import config
from orinoco.action import (
    Action,
    record_action,
    verbose_action_exception,
    async_record_action,
    async_verbose_action_exception,
)
from orinoco.condition import Condition
from orinoco.entities import ActionConfig, Signature
from orinoco.exceptions import ActionNotProperlyConfigured
from orinoco.helpers import extract_type
from orinoco.types import T, ActionDataT


class TypedBase(Generic[T], Action, ABC):
    CONFIG: Optional[ActionConfig[T]] = None

    __call__: Callable[..., Any]

    def __init__(
        self, description: Optional[str] = None, config: Optional[ActionConfig[T]] = None, name: Optional[str] = None
    ):
        Action.__init__(self, description=description, name=name)

        self.config = config or self.CONFIG or self._get_implicit_config()

    def get_input_params(self, action_data: ActionDataT) -> Dict[str, Any]:
        return {key: action_data.find_one(signature) for key, signature in (self.config.INPUT or {}).items()}

    @classmethod
    def _get_implicit_config(cls) -> ActionConfig[T]:
        annotations = cls.__call__.__annotations__
        return_type, return_name, tags = extract_type(annotations["return"])

        if config.IMPLICIT_TYPE_STRICT_MODE_ENABLED and return_name is None:
            raise ActionNotProperlyConfigured(
                "Action {} has to be configured explicitly or return type has to be annotated via "
                "`Annotated[<type>, <name>]`. The error was raised, because `IMPLICIT_TYPE_STRICT_MODE_ENABLED` "
                "is enabled.".format(cls)
            )

        return ActionConfig(
            OUTPUT=Signature(type_=return_type, key=return_name, tags=tags) if return_type else None,
            INPUT={key: Signature(key=key) for key, type_ in annotations.items() if key != "return"},
        )


class TypedAction(Generic[T], TypedBase[T], ABC):
    """
    Enhanced `Action` which use `ActionConfig` as configuration of the input and the output.

    Business logic is defined in the call method with "normal" parameters as the input, this means that no raw
    `ActionData` is required. The propagation of the data from the `ActionData` to the method is done automatically
    based on the `ActionConfig` which can be passed directly to the constructor (see `config`) or as the class variable
    (see `CONFIG`) or implicitly from annotations of the `__call__` method.

    The result of the method is propagated to the `ActionData` with the corresponding signature. Note that implicit
    config uses only annotated return type for the signature. For more control define the `ActionConfig` manually.
    """

    __call__: Callable[..., T]

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        result = self(**self.get_input_params(action_data))

        if self.config.OUTPUT:
            return action_data.register(signature=self.config.OUTPUT, entity=result)
        return action_data


class AsyncTypedAction(Generic[T], TypedBase[T], ABC):
    """
    Async version of :class:`TypedAction`
    """

    SYNC_ACTION: Optional[Type[TypedAction[T]]] = None

    __call__: Callable[..., Awaitable[T]]

    def __init__(
        self,
        description: Optional[str] = None,
        config: Optional[ActionConfig[T]] = None,
        name: Optional[str] = None,
        sync_action_kwargs: Optional[Dict[str, Any]] = None,
    ):
        super(AsyncTypedAction, self).__init__(description=description, config=config, name=name)

        self.sync_action = self.SYNC_ACTION(**(sync_action_kwargs or {})) if self.SYNC_ACTION else None

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        result = await self(**self.get_input_params(action_data))

        if self.config.OUTPUT:
            return action_data.register(signature=self.config.OUTPUT, entity=result)
        return action_data

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if not self.sync_action:
            raise ActionNotProperlyConfigured(
                "SYNC_ACTION action need to be provided in order to run this async action synchronously"
            )
        return self.sync_action.run(action_data)


class TypedCondition(Condition, TypedBase[bool], ABC):
    """
    Condition base of :class:`TypedAction` type
    """

    __call__: Callable[..., bool]

    def __init__(
        self,
        fail_message: Optional[str] = None,
        is_inverted: bool = False,
        error_cls: Optional[Type[Exception]] = None,
        config: Optional[ActionConfig[bool]] = None,
        name: Optional[str] = None,
    ):
        TypedBase.__init__(self, config=config)
        Condition.__init__(self, fail_message=fail_message, is_inverted=is_inverted, error_cls=error_cls, name=name)

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return self(**self.get_input_params(action_data))
