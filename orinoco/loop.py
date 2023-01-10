from abc import ABC, abstractmethod
from typing import Iterable, Any, Callable, Optional, TypeVar, Generic, AsyncIterable

from orinoco.action import (
    Action,
    ActionSet,
    record_action,
    verbose_action_exception,
    async_record_action,
    async_verbose_action_exception,
)

from orinoco.exceptions import ActionNotProperlyConfigured, RunnableOnlyInAsyncContext
from orinoco.types import ActionDataT

LoopT = TypeVar("LoopT", bound="BaseLoop")


class BaseLoop(Generic[LoopT], Action, ABC):
    def __init__(self, iterating_key: str, action: Optional[Action] = None):
        """
        :param iterating_key: Key which will be propagated into the :class:`~orinoco.entities.ActionData` with the
        new value
        """
        super().__init__(description="Loop over '{}'".format(iterating_key))
        self.iterating_key = iterating_key

        self.action = action

    def do(self: LoopT, *actions: Action) -> LoopT:
        """
        :param actions: Actions which are executed in the each iteration with updated
         values in the  :class:`~orinoco.entities.ActionData`
        :return:
        """
        self.action = ActionSet(actions)
        return self


class LoopEvents(BaseLoop):
    """
    Loop implementation for actions which runs just side-effects. Values obtained in the loop are
    not propagated further in the actions chain
    """

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if not self.action:
            raise ActionNotProperlyConfigured(
                "Action for the loops is not set. Call `do` method first or provide it through the constructor"
            )
        for loop_data in self._get_data_generator(action_data):
            self.action.run(loop_data)
        return action_data

    def _get_data_generator(self, action_data: ActionDataT) -> Iterable[ActionDataT]:
        for value in self._get_generator(action_data):
            yield action_data.evolve(**{self.iterating_key: value})

    @abstractmethod
    def _get_generator(self, action_data: ActionDataT) -> Iterable[Any]:
        """
        :param action_data: Data container passed by actions
        :return: Iterable which provides the value which is set to the :class:`~orinoco.entities.ActionData`
        in each of the iterations
        """
        pass


class ForSideEffects(LoopEvents):
    """
    Implementation of `Loop` which uses a method for generating loop iterable

    Example:

    .. code-block:: python

        ForSideEffects("event", lambda action_data: action_data.get_or_default("event_log", [])).do(GetNotification())

    Then `GetNotification` has access to `event` via action data in each of the iterations
    which comes from the iterable "event_log"
    """

    def __init__(self, iterating_key: str, method: Callable[[ActionDataT], Iterable[Any]]):
        """
        :param iterating_key: Key which will be propagated into the `ActionData` with the new value
        :param method: Method which returns the iterable to iterate over
        """
        super().__init__(iterating_key=iterating_key)
        self.method = method

    def _get_generator(self, action_data: ActionDataT) -> Iterable[ActionDataT]:
        return self.method(action_data)


class For(BaseLoop):
    """
    Implementation of `Loop` which uses a method for generating loop iterable

    Example:

    .. code-block:: python

        For("x", lambda ad: ad.get("values"), aggregated_field="doubled", aggregated_field_new_name="doubled_list")
        .do(DoubleValue())
        .run_with_data(values=[10, 40, 60])
        .get("doubled_list")
    """

    def __init__(
        self,
        iterating_key: str,
        method: Callable[[ActionDataT], Iterable[Any]],
        aggregated_field: Optional[str] = None,
        aggregated_field_new_name: Optional[str] = None,
        skip_none_for_aggregated_field: bool = False,
    ):
        """
        :param iterating_key: Key which will be propagated into the `ActionData` with the new value
        :param method: Method which returns the iterable to iterate over
        :param aggregated_field: Name of the field which will be extracted from the `ActionData` and aggregated
        (appended to the list)
        :param aggregated_field_new_name: Name of the field which will be used for the aggregated field
        :param skip_none_for_aggregated_field: If `True` then `None` values won't be added to the aggregated field
        """
        super().__init__(iterating_key=iterating_key)
        self.method = method
        self.aggregated_field = aggregated_field
        self.aggregated_field_new_name = aggregated_field_new_name
        self.skip_none_for_aggregated_field = skip_none_for_aggregated_field

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if not self.action:
            raise ActionNotProperlyConfigured(
                "Action for the loops is not set. Call `do` method first or provide it through the constructor"
            )

        aggregated_values = []
        for iteration_value in self.method(action_data):
            loop_action_data = self.action.run(action_data.evolve(**{self.iterating_key: iteration_value}))
            if self.aggregated_field:
                value_to_aggregate = action_data.get(self.aggregated_field)
                if value_to_aggregate is not None or (not self.skip_none_for_aggregated_field):
                    aggregated_values.append(value_to_aggregate)

        if self.aggregated_field:
            return loop_action_data.evolve(
                **{self.aggregated_field_new_name or self.aggregated_field: aggregated_values}
            )
        return action_data


class AsyncLoopEvents(LoopEvents):
    """
    Loop implementation for actions which runs just side-effects/conditions. Values obtained in the loop are
    not propagated further in the actions chain
    """

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if not self.action:
            raise ActionNotProperlyConfigured(
                "Action for the loops is not set. Call `do` method first or provide it through the constructor"
            )

        async for loop_data in self._get_async_data_generator(action_data):
            await self.action.async_run(loop_data)
        return action_data

    async def _get_async_data_generator(self, action_data: ActionDataT) -> AsyncIterable[ActionDataT]:
        async for value in self._get_generator(action_data):
            yield action_data.evolve(**{self.iterating_key: value})

    @abstractmethod
    def _get_generator(self, action_data: ActionDataT) -> AsyncIterable[Any]:
        """
        :param action_data: Data container passed by actions
        :return: Iterable which provides the value which is set to the :class:`~orinoco.entities.ActionData`
        in each of the iterations
        """
        pass


class AsyncForSideEffects(AsyncLoopEvents):
    """
    Implementation of `Async Loop` which uses a method for generating loop async iterable

    Example:

    .. code-block:: python

        For("event", lambda action_data: action_data.get_or_default("event_log", [])).do(GetNotification())

    Then `GetNotification` has access to `event` via action data in each of the iterations
    which comes from the async iterable "event_log"
    """

    def __init__(self, iterating_key: str, method: Callable[[ActionDataT], AsyncIterable[Any]]):
        """
        :param iterating_key: Key which will be propagated into the `ActionData` with the new value
        :param method: Method which returns the iterable to iterate over
        """
        super().__init__(iterating_key=iterating_key)
        self.method = method

    def _get_generator(self, action_data: ActionDataT) -> AsyncIterable[ActionDataT]:
        return self.method(action_data)


class AsyncFor(BaseLoop):
    def run(self, action_data: ActionDataT) -> ActionDataT:
        raise RunnableOnlyInAsyncContext()

    def __init__(
        self,
        iterating_key: str,
        method: Callable[[ActionDataT], AsyncIterable[Any]],
        aggregated_field: Optional[str] = None,
        aggregated_field_new_name: Optional[str] = None,
        skip_none_for_aggregated_field: bool = False,
    ):
        """
        :param iterating_key: Key which will be propagated into the `ActionData` with the new value
        :param method: Method which returns the iterable to iterate over
        :param aggregated_field: Name of the field which will be extracted from the `ActionData` and aggregated
        (appended to the list)
        :param aggregated_field_new_name: Name of the field which will be used for the aggregated field
        :param skip_none_for_aggregated_field: If `True` then `None` values won't be added to the aggregated field
        """
        super().__init__(iterating_key=iterating_key)
        self.method = method
        self.aggregated_field = aggregated_field
        self.aggregated_field_new_name = aggregated_field_new_name
        self.skip_none_for_aggregated_field = skip_none_for_aggregated_field

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if not self.action:
            raise ActionNotProperlyConfigured(
                "Action for the loops is not set. Call `do` method first or provide it through the constructor"
            )

        aggregated_values = []
        async for iteration_value in self.method(action_data):
            action_data = await self.action.async_run(action_data.evolve(**{self.iterating_key: iteration_value}))
            if self.aggregated_field:

                value_to_aggregate = action_data.get(self.aggregated_field)
                if value_to_aggregate is not None or (not self.skip_none_for_aggregated_field):
                    aggregated_values.append(value_to_aggregate)

        if self.aggregated_field:
            return action_data.evolve(**{self.aggregated_field_new_name or self.aggregated_field: aggregated_values})
        return action_data
