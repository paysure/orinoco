from abc import ABC, abstractmethod
from typing import Iterable, Any, Callable, Optional, TypeVar, Generic

from orinoco.action import Action, ActionSet, record_action, verbose_action_exception

from orinoco.exceptions import ActionNotProperlyConfigured
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
    Loop implementation for actions which runs just side-effects/conditions. Values obtained in the loop are
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

        For("event", lambda action_data: action_data.get_or_default("event_log", [])).do(GetNotification())

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
    def __init__(
        self,
        iterating_key: str,
        method: Callable[[ActionDataT], Iterable[Any]],
        aggregated_field: Optional[str] = None,
        aggregated_field_new_name: Optional[str] = None,
    ):
        """
        :param iterating_key: Key which will be propagated into the `ActionData` with the new value
        :param method: Method which returns the iterable to iterate over
        """
        super().__init__(iterating_key=iterating_key)
        self.method = method
        self.aggregated_field = aggregated_field
        self.aggregated_field_new_name = aggregated_field_new_name

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
            action_data = self.action.run(action_data.evolve(**{self.iterating_key: iteration_value}))
            if self.aggregated_field:
                aggregated_values.append(action_data.get(self.aggregated_field))

        if self.aggregated_field:
            return action_data.evolve(**{self.aggregated_field_new_name or self.aggregated_field: aggregated_values})
        return action_data
