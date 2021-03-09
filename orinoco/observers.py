import abc
import time
from typing import List, Tuple, Dict

from orinoco.tags import SystemActionTag
from orinoco.types import ObserverT, ActionT


class Observer(ObserverT, abc.ABC):
    def should_record_action(self, action: ActionT) -> bool:
        """
        Controls whether the observer will log given action
        """
        return True

    def record_start(self, action: ActionT) -> None:
        """
        This method is executed before the ``action`` is executed
        """
        pass

    def record_end(self, action: ActionT) -> None:
        """
        This method is executed after the ``action`` is executed
        """
        pass


class ExecutionTimeObserver(Observer):
    """
    Observer which measures execution times of the actions in the pipeline
    """

    measurements: List[Tuple[str, float]]

    def __init__(self) -> None:
        self.measurements = []
        self._measuring: Dict[ActionT, float] = {}

    def __repr__(self) -> str:
        return "ExecutionTimeObserver({})".format(self.measurements)

    def should_record_action(self, action: ActionT) -> bool:
        return SystemActionTag not in action.__class__.__bases__

    def record_start(self, action: ActionT) -> None:
        self._measuring[action] = time.time()

    def record_end(self, action: ActionT) -> None:
        self.measurements.append(("action.{}".format(action.action_name), time.time() - self._measuring.pop(action)))


class ActionsLog(Observer):
    """
    Observer which logs which actions were executed
    """

    actions_log: List[str]

    def __init__(self) -> None:
        self.actions_log = []

    def __repr__(self) -> str:
        return "ActionsLog({})".format(self.actions_log)

    def record_start(self, action: ActionT) -> None:
        self.actions_log.append(
            (
                action.action_name + "_start"
                if "AND" not in action.action_name
                else "({})_start".format(action.action_name)
            )
        )

    def record_end(self, action: ActionT) -> None:
        self.actions_log.append(
            action.action_name + "_end" if "AND" not in action.action_name else "({})_end".format(action.action_name)
        )
