from abc import ABC, abstractmethod
from typing import Iterable, Any, Callable, Optional, TypeVar

from lena.action import Action, ActionSet, record_action, verbose_action_exception

from lena.exceptions import ActionNotProperlyConfigured
from lena.types import ActionDataT

LoopT = TypeVar("LoopT", bound="LoopEvents")


class LoopEvents(Action, ABC):
    """
    Loop implementation for actions which runs just side-effects/conditions. Values obtained in the loop are
    not propagated further in the actions chain
    """

    def __init__(self, iterating_key: str, action: Optional[Action] = None):
        """
        :param iterating_key: Key which will be propagated into the :class:`~lena.entities.ActionData` with the
        new value
        """
        super().__init__(description="Loop over '{}'".format(iterating_key))
        self.iterating_key = iterating_key

        self.action = action

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

    def do(self: LoopT, *actions: Action) -> LoopT:
        """
        :param actions: Actions which are executed in the each iteration with updated
         values in the  :class:`~lena.entities.ActionData`
        :return:
        """
        self.action = ActionSet(actions)
        return self

    def _get_data_generator(self, action_data: ActionDataT) -> Iterable[ActionDataT]:
        for value in self._get_generator(action_data):
            yield action_data.evolve(**{self.iterating_key: value})

    @abstractmethod
    def _get_generator(self, action_data: ActionDataT) -> Iterable[Any]:
        """
        :param action_data: Data container passed by actions
        :return: Iterable which provides the value which is set to the :class:`~lena.entities.ActionData`
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
