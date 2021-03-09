from abc import ABC
from typing import Any, Callable, Union, Optional

from orinoco.action import (
    Action,
    record_action,
    verbose_action_exception,
    ActionSet,
    async_record_action,
    async_verbose_action_exception,
)
from orinoco.entities import ActionData
from orinoco.exceptions import ActionNotProperlyInherited
from orinoco.helpers import raise_not_provided_field
from orinoco.types import ActionDataT


class DataSource(Action, ABC):
    """
    Base action which provides new data
    """

    PROVIDES: Optional[str] = None

    def __init__(
        self,
        provides: Optional[str] = None,
        dont_get_if_is_in: bool = False,
        description: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """
        :param provides: Key in :class:`~orinoco.entities.ActionData` under the data it will be stored.
        If it's not provided :attrs:`orinoco.data_source.DataSource.PROVIDES` is used as default value.
        :param dont_get_if_is_in: If it's True and a value with ``provides`` key is already
        in `:class:`~orinoco.entities.ActionData` the :func:`~orinoco.data_source.DataSource.get_data` is not executed.
        :param description: Description of the action
        :param name: Name of the action
        """
        super().__init__(name=name, description=description)
        self.provides = provides or self.PROVIDES or raise_not_provided_field("provides")
        self.dont_get_if_is_in = dont_get_if_is_in

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if self.dont_get_if_is_in and action_data.is_in(self.provides):
            return action_data
        return action_data.evolve(**{self.provides: self.get_data(action_data)})

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if self.dont_get_if_is_in and action_data.is_in(self.provides):
            return action_data
        return action_data.evolve(**{self.provides: await self.async_get_data(action_data)})

    def get_data(self, action_data: ActionDataT) -> Any:
        """
        :param action_data:
        :return: Value which is appended to `ActionData`
        """
        raise ActionNotProperlyInherited(
            "`get_data` method needs to implemented for sync execution or `async_get_data` for async execution"
        )

    async def async_get_data(self, action_data: ActionDataT) -> Any:
        """
        :param action_data: Data container passed by actions
        :return: Value which is appended to `~orinoco.entities.ActionData`
        """
        return self.get_data(action_data)

    def get_source_data(self, **kwargs: Any) -> Any:
        """
        Shortcut to call the :class:`~orinoco.data_source.DataSource` instance directly and get the result

        :param kwargs: Data which are propagated into `~orinoco.entities.ActionData`
        :return: Requested data value
        """
        return self.get_data(ActionData.create(**kwargs))


class GenericDataSource(DataSource):
    """
    Utility data source action which allows inserting values into the `~orinoco.entities.ActionData` "on the fly"
    (usually by lambda functions)
    """

    def __init__(
        self, method: Callable[[ActionDataT], Any], provides: Optional[str] = None, name: Optional[str] = None
    ):
        """
        :param method: Method which use :class:`~orinoco.entities.ActionData` to get the data
        :param provides:
        """
        super().__init__(provides=provides, name=name)
        self.method = method

    def get_data(self, action_data: ActionDataT) -> Any:
        return self.method(action_data)


class AddActionValue(DataSource):
    """
    Utility class which adds a value into the :class:`~orinoco.entities.ActionData`
    """

    def __init__(self, key: str, value: Union[Any, Callable[[], Any]]):
        """
        :param key: Key of the value in the :class:`~orinoco.entities.ActionData`
        :param value: Value to set
        """
        super().__init__(provides=key, name="{}[{}]".format(self.__class__.__name__, key))
        self.value = value

    def get_data(self, action_data: ActionDataT) -> Any:
        if callable(self.value):
            return self.value()

        return self.value


class AddActionValues(ActionSet):
    def __init__(self, **action_values: Any):
        super().__init__([AddActionValue(k, v) for k, v in action_values.items()])


class AddVirtualKeyShortcut(GenericDataSource):
    def __init__(self, key: str, source_key: str):
        """
        :param key: New key in the :class:`~orinoco.entities.ActionData`
        :param source_key: Key for getting the value from :class:`~orinoco.entities.ActionData`
        """
        super().__init__(
            provides=key,
            method=lambda ad: ad.get(source_key),
            name="{}[{} -> {}]".format(self.__class__.__name__, source_key, key),
        )
