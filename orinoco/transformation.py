from abc import ABC
from typing import Callable, Optional

from orinoco import config
from orinoco.action import (
    Action,
    record_action,
    verbose_action_exception,
    async_record_action,
    async_verbose_action_exception,
)
from orinoco.entities import ActionData, Signature
from orinoco.exceptions import ActionNotReturnedActionData, ActionNotProperlyInherited
from orinoco.types import ActionDataT


class Transformation(Action, ABC):
    """
    Base class for actions which modify data
    """

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        return self._check_transformation_output(self.transform(action_data))

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        return self._check_transformation_output(await self.async_transform(action_data))

    def transform(self, action_data: ActionDataT) -> ActionDataT:
        raise ActionNotProperlyInherited(
            "`transform` method needs to implemented for sync execution or `async_transform` for async execution"
        )

    async def async_transform(self, action_data: ActionDataT) -> ActionDataT:
        return self.transform(action_data)

    @classmethod
    def _check_transformation_output(cls, action_data: ActionDataT) -> ActionDataT:
        if config.CHAINING_TYPE_CHECK_STRICT_MODE_ENABLED and not isinstance(action_data, ActionData):
            raise ActionNotReturnedActionData(
                "Action {} not returned ActionData, but {}".format(action_data, cls.__name__)
            )
        return action_data


class GenericTransformation(Transformation):
    """
    Utility action which allows modification of `~orinoco.entities.ActionData` "on the fly"
    (usually by lambda functions)
    """

    def __init__(
        self,
        method: Callable[[ActionDataT], ActionDataT],
        description: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """
        :param method: Method which takes `ActionData`, modify them and return new copy
        :param description:
        """
        super().__init__(description=description, name=name)
        self.method = method

    def transform(self, action_data: ActionDataT) -> ActionDataT:
        action_data_result = self.method(action_data)

        if config.CHAINING_TYPE_CHECK_STRICT_MODE_ENABLED and not isinstance(action_data_result, ActionData):
            raise ActionNotReturnedActionData(
                "Action {} not returned ActionData, but {}".format(action_data_result, self.__class__.__name__)
            )

        return action_data_result


class RenameActionField(Transformation):
    """
    Change name of the registered entity
    """

    def __init__(self, key: str, new_key: str):
        super().__init__(name="{}[{} -> {}]".format(self.__class__.__name__, key, new_key))
        self.key = key
        self.new_key = new_key

    def transform(self, action_data: ActionDataT) -> ActionDataT:
        new_data = []
        for signature, value in action_data.data:
            if signature.key == self.key:
                new_data.append((signature.evolve_self(key=self.new_key), value))
            else:
                new_data.append((signature, value))
        return action_data.evolve_self(data=new_data)


class WithoutFields(Transformation):
    """
    Remove signatures for the given keys
    """

    def __init__(self, *fields: str):
        super().__init__(name="{}[{}]".format(self.__class__.__name__, ", ".join(fields)))
        self.fields = fields

    def transform(self, action_data: ActionDataT) -> ActionDataT:
        return action_data.remove_many(
            searched_signatures=[Signature(key=field) for field in self.fields],
            exact_match=False,
            ignore_non_existent=False,
        )
