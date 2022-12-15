import json
import sys
from abc import ABC, abstractmethod
from functools import wraps
from typing import (
    Iterable,
    Callable,
    Union,
    Type,
    Optional,
    Any,
    Dict,
    ContextManager,
    AsyncContextManager,
    Tuple,
    NoReturn,
    Coroutine,
)

from orinoco import config
from orinoco.entities import ActionData
from orinoco.exceptions import ActionNotProperlyConfigured, BaseActionException
from orinoco.observers import ActionsLog
from orinoco.tags import SystemActionTag
from orinoco.types import ActionT, ActionDataT, NamespacedActionT, ActionVar


class _NamespacedAction(NamespacedActionT):
    """
    Wrapper class named actions. See `Action.namespaced`
    """

    def __init__(self, class_to_return: Type[ActionT]):
        self.class_to_return = class_to_return

    def __getattr__(self, attribute: Any) -> Type[ActionT]:
        self.class_to_return.name = attribute
        return self.class_to_return


class SyncActionMixin(ABC):
    @abstractmethod
    def run(self, action_data: ActionDataT) -> ActionDataT:
        """
        Method which is executed when running the action pipeline

        Note that each override of this function have to implement "skipping logic" to support :class:`~Return`
        functionality.

        .. code-block:: python

            def run(self, action_data: ActionDataT) -> ActionDataT:
                if action_data.skip_processing:
                    return action_data

                # normal implementation of the action
                ...

        :param action_data: Data container passed by actions
        :return: Last version of :class:`:class:~orinoco.entities.ActionData` after execution
        """
        pass

    def run_with_data(self, **params: Any) -> ActionDataT:
        """
        Shortcut for running actions without creating :class:`~orinoco.entities.ActionData` object directly

        :param params: Parameters which are propagated into :class:`~orinoco.entities.ActionData`
        :return: Processed data with results
        """
        return self.run(ActionData.create(**params))


class AsyncActionMixin(ABC):
    @abstractmethod
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        """
        Method which is executed when running the action pipeline

        Note that each override of this function have to implement "skipping logic" to support :class:`~Return`
        functionality.

        .. code-block:: python

            def run(self, action_data: ActionDataT) -> ActionDataT:
                if action_data.skip_processing:
                    return action_data

                # normal implementation of the action
                ...

        :param action_data: Data container passed by actions
        :return: Last version of `~orinoco.entities.ActionData` after execution
        """
        pass

    async def async_run_with_data(self, **params: Any) -> ActionDataT:
        """
        Shortcut for running actions asynchronously without creating :class:`~orinoco.entities.ActionData` object directly

        :param params: Parameters which are propagated into :class:`~orinoco.entities.ActionData`
        :return: Processed data with results
        """
        return await self.async_run(ActionData.create(**params))


class Action(SyncActionMixin, AsyncActionMixin, ActionT, ABC):
    """
    Fundamental entity which is designed to be chained with other actions so one action process
    the :class:`~orinoco.entities.ActionData` and send it to the next action.
    """

    DESCRIPTION: str = ""
    NAME: str = ""

    def __init__(self, description: Optional[str] = None, name: Optional[str] = None):
        """
        :param description: Description of the task. Is used when an exception is raised, for debugging and
        increase of readability. It has no effect for runtime. Can be also set as class attribute if it's
        not provided to constructor
        """
        self.description = description or self.DESCRIPTION
        self.name = name or self.NAME or self.__class__.__name__

    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        return self.run(action_data)

    def then(self, another_action: "ActionT") -> "ActionT":
        """
        One of the chaining functions. More convenient way for chaining is to use `>>` operator like
        this:

        .. code-block:: python

            Action1() >> Action2()

        :param another_action: Another action which will be executed after
        :return: Set of this action and ``another_action`` represented by :class:`ActionSet`
        """
        return ActionSet([self, another_action])

    def __rshift__(self, other: ActionT) -> "ActionT":
        return self.then(other)

    def on_subfield(self, field_key: str) -> "OnActionDataSubField":
        """
        Run action on a subfield. See :class:`OnActionDataSubField` class for more information.

        :param field_key: Key to the item in :class:`~orinoco.entities.ActionData` on which the action will be executed
        :return: Wrapped action which will be executed on the subfield
        """
        return OnActionDataSubField(self, field_key)

    @property
    def action_name(self) -> str:
        """
        :return: Name of the action
        """
        return self.name or self.__class__.__name__

    @classmethod
    def namespaced(cls) -> NamespacedActionT:
        """
        Return 'named' instance of itself. It has no effect, just make a 'title' for the action/s by wrapping them
        into a named group of actions.


        Example:

        .. code-block:: python

            EventSet.namespaced().MarkPaymentAttemptAsProcessed(
                    [SetPaymentAuthorizationAttemptProcessed(), SaveModel("authorization_attempt")]
                )

        `MarkPaymentAttemptAsProcessed` is just a "made up" key of the following actions.
        """
        return _NamespacedAction(cls)

    def finish(self) -> "Return":
        return Return(self)


def record_action(
    fu: Callable[[ActionVar, ActionDataT], ActionDataT]
) -> Callable[[ActionVar, ActionDataT], ActionDataT]:
    """
    Decorator for :func:`~SyncActionMixin.run` which records decorated action signature to
    the :class:`~orinoco.entities.ActionData`

    :param fu: :func:`~SyncActionMixin.run` method (or any with the same signature)
    :return: Decorated function
    """

    @wraps(fu)
    def wrapper(action: ActionVar, action_data: ActionDataT) -> ActionDataT:
        if config.CHAINING_TYPE_CHECK_STRICT_MODE_ENABLED and not isinstance(action_data, ActionData):
            raise ActionNotProperlyConfigured(
                "Input argument of {} - {} must of type ActionData, but got {}".format(
                    action.action_name, fu, action_data
                )
            )

        result = fu(action, action_data.record_start(action))

        if config.CHAINING_TYPE_CHECK_STRICT_MODE_ENABLED and not isinstance(result, ActionData):
            raise ActionNotProperlyConfigured(
                "Output of {} - {} must of type ActionData, but got {}".format(action.action_name, fu, result)
            )

        return result.record_end(action)

    return wrapper


def async_record_action(
    fu: Callable[[ActionVar, ActionDataT], Coroutine[None, None, ActionDataT]]
) -> Callable[[ActionVar, ActionDataT], Coroutine[None, None, ActionDataT]]:
    """
    Decorator for :func:`~AsyncActionMixin.async_run` which records decorated action signature to
    the :class:`~orinoco.entities.ActionData`

    :param fu: :func:`~AsyncActionMixin.async_run` method (or any with the same signature)
    :return: Decorated function
    """

    @wraps(fu)
    async def wrapper(action: ActionVar, action_data: ActionDataT) -> ActionDataT:
        if config.CHAINING_TYPE_CHECK_STRICT_MODE_ENABLED and not isinstance(action_data, ActionData):
            raise ActionNotProperlyConfigured(
                "Input argument of {} - {} must of type ActionData, but got {}".format(
                    action.action_name, fu, action_data
                )
            )

        result = await fu(action, action_data.record_start(action))

        if config.CHAINING_TYPE_CHECK_STRICT_MODE_ENABLED and not isinstance(result, ActionData):
            raise ActionNotProperlyConfigured(
                "Output of {} - {} must of type ActionData, but got {}".format(action.action_name, fu, result)
            )

        return result.record_end(action)

    return wrapper


def verbose_action_exception(
    fu: Callable[[ActionVar, ActionDataT], ActionDataT]
) -> Callable[[ActionVar, ActionDataT], ActionDataT]:
    """
    Decorator which adds some more context information to errors raised during action execution.

    :param fu: :func:`~SyncActionMixin.run` method (or any with the same signature)
    :return: Decorated function
    """

    @wraps(fu)
    def wrapper(action: ActionVar, action_data: ActionDataT) -> ActionDataT:
        try:
            return fu(action, action_data)
        except Exception as err:
            _raise_new_error(action=action, err=err, action_data=action_data)

    return wrapper


def async_verbose_action_exception(
    fu: Callable[[ActionVar, ActionDataT], Coroutine[None, None, ActionDataT]]
) -> Callable[[ActionVar, ActionDataT], Coroutine[None, None, ActionDataT]]:
    """
     Decorator which adds some more context information to errors raised during action execution.

    :param fu: :func:`~AsyncActionMixin.async_run` method (or any with the same signature)
    :return: Decorated function
    """

    @wraps(fu)
    async def wrapper(action: ActionVar, action_data: ActionDataT) -> ActionDataT:
        try:
            return await fu(action, action_data)
        except Exception as err:
            _raise_new_error(action=action, err=err, action_data=action_data)

    return wrapper


def _raise_new_error(action: ActionT, err: Exception, action_data: ActionDataT) -> NoReturn:
    if config.VERBOSE_ERRORS and (len(err.args) == 0 or (err.args and "Action context" not in err.args[0])):
        fields = [
            ("Actions history", json.dumps(action_data.get_observer(ActionsLog).actions_log, indent=4)),
            ("Actions data", json.dumps({str(k): str(v) for k, v in action_data.data}, indent=4)),
            (
                "{} params".format(action.action_name),
                json.dumps({k: str(v) for k, v in action.__dict__.items()}, indent=4),
            ),
        ]

        context_message = "\n".join("{}: {}".format(field[0], field[1]) for field in fields)

        msg = "{0}\n{1} Action context {1}\n{2}".format(str(err).strip(), "-" * 20, context_message)
        try:
            raise err.__class__(msg).with_traceback(sys.exc_info()[2]) from err
        except TypeError:
            raise BaseActionException(msg)
    else:
        raise


class ActionSet(Action, SystemActionTag):
    """
    Set of actions which are executed consequently
    """

    ACTIONS: Optional[Iterable[ActionT]] = None

    def __init__(
        self, actions: Optional[Iterable[ActionT]] = None, description: Optional[str] = None, name: Optional[str] = None
    ):
        super().__init__(description=description, name=name)
        self.actions = actions or self.ACTIONS or []

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        for action in self.actions:
            if action_data.skip_processing:
                return action_data

            action_data = action.run(action_data)
        return action_data

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        for action in self.actions:
            if action_data.skip_processing:
                return action_data

            action_data = await action.async_run(action_data)
        return action_data


class AtomicActionSet(ActionSet, SystemActionTag):
    """
    Set of actions which are executed in a context manager
    """

    def __init__(
        self,
        actions: Iterable[ActionT],
        atomic_context_manager: Callable[[], ContextManager[None]],
        description: Optional[str] = None,
        name: Optional[str] = None,
    ):
        super().__init__(actions=actions, description=description, name=name)
        self.atomic_context_manager = atomic_context_manager

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        with self.atomic_context_manager():
            for action in self.actions:
                if action_data.skip_processing:
                    return action_data

                action_data = action.run(action_data)
            return action_data


class AsyncAtomicActionSet(ActionSet, SystemActionTag):
    """
    Async version of :class:`AtomicActionSet`.
    """

    def __init__(
        self,
        actions: Iterable[ActionT],
        atomic_context_manager: Callable[[], AsyncContextManager[None]],
        description: Optional[str] = None,
        name: Optional[str] = None,
    ):
        super().__init__(actions=actions, description=description, name=name)
        self.atomic_context_manager = atomic_context_manager

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        async with self.atomic_context_manager():
            for action in self.actions:
                if action_data.skip_processing:
                    return action_data

                action_data = await action.async_run(action_data)
            return action_data


class Then(ActionSet, SystemActionTag):
    """
    Syntactics sugar for :class:`ActionSet`. See :class:`~orinoco.condition.Switch` docs for usage
    """

    def __init__(self, *actions: ActionT):
        super().__init__(actions)


class HandledExceptions(ActionSet, SystemActionTag):
    """
    Try-except wrapper for actions
    """

    def __init__(
        self,
        *actions: ActionT,
        catch_exceptions: Union[Tuple[Type[BaseException], ...], Type[BaseException]],
        handle_method: Callable[[BaseException, ActionDataT], Optional[Union[Type[BaseException], BaseException]]],
        fail_on_error: bool = True,
        name: Optional[str] = None,
    ):
        """
        :param actions: Actions to execute
        :param catch_exceptions: Errors to except
        :param handle_method: Method to execute when the actions fail
        :param fail_on_error: If true fail on error, otherwise the execution will continue
        :param name: Name of the action
        """

        super().__init__(actions=actions, name=name)
        self.catch_exceptions = catch_exceptions
        self.handle_method = handle_method
        self.fail_on_error = fail_on_error

    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data
        try:
            return super().run(action_data)
        except self.catch_exceptions as err:
            new_err = self.handle_method(err, action_data)
            if not self.fail_on_error:
                return action_data
            if isinstance(new_err, BaseException):
                raise new_err
            raise err

    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        try:
            return await super(ActionSet, self).async_run(action_data=action_data)
        except self.catch_exceptions as err:
            new_err = self.handle_method(err, action_data)

            if not self.fail_on_error:
                return action_data
            if isinstance(new_err, BaseException):
                raise new_err
            raise err


class OnActionDataSubField(Action, SystemActionTag):
    """
    Utility action to execute an action on values of a nested item.

    For example if an action accepts parameter `user`, but action data contains only `request_data` dictionary with
    the desired `user`.

    .. code-block:: python

        action = OnActionDataSubField(DoSomethingWithUser(), field_key="request_data")

        # Same as
        action = DoSomethingWithUser().on_subfield("request_data")

    As shown above :func:`~Action.on_subfield` can be used as a shortcut.
    """

    def __init__(self, action: ActionT, field_key: str, name: Optional[str] = None):
        super().__init__(name=name or "{}[{}({})]".format(self.__class__.__name__, action.action_name, field_key))
        self.action = action
        self.field_key = field_key

    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        return self._with_subfield_data(
            action_data, self.action.run(action_data.with_new_data(action_data.get(self.field_key)))
        )

    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        return self._with_subfield_data(
            action_data, await self.action.async_run(action_data.with_new_data(action_data.get(self.field_key)))
        )

    def _with_subfield_data(self, action_data: ActionDataT, new_action_data: ActionDataT) -> ActionDataT:
        return action_data.evolve(**self._unwrap_nested_key(self.field_key, new_action_data.as_keyed_dict()))

    @classmethod
    def _unwrap_nested_key(cls, key: str, value: Any) -> Dict[str, Any]:
        last_dot_index = key.rfind(".")
        if last_dot_index > 0:
            return cls._unwrap_nested_key(key[:last_dot_index], value={key[last_dot_index + 1 :]: value})

        return {key: value}


class Return(Action):
    """
    Mark action data as "should not be processed", imitating return statement in functions
    """

    def __init__(self, action: Optional[ActionT] = None):
        super().__init__()
        self.action = action

    def run(self, action_data: ActionDataT) -> ActionDataT:
        return (self.action.run(action_data) if self.action else action_data).evolve_self(skip_processing=True)
