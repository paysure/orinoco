import copy
import json
from abc import abstractmethod, ABC
from typing import Tuple, Callable, Any, Iterable, List, Generator, Union, Optional, Type, NoReturn, TypeVar, Generic

from orinoco.action import (
    Action,
    record_action,
    verbose_action_exception,
    Then,
    async_record_action,
    async_verbose_action_exception,
)
from orinoco.entities import ActionData, Signature
from orinoco.exceptions import NoneOfActionsCanBeExecuted, ConditionNotMet
from orinoco.helpers import raise_not_provided_field, is_format_string, get_format_string_args
from orinoco.tags import SystemActionTag
from orinoco.types import T, ActionDataT, ActionT

ConditionT = TypeVar("ConditionT", bound="Condition")


class Condition(Action, ABC):
    """
    Base class for condition base actions. In the action set they are treated as checks which raise an exception
    when the condition is not met. However, can be used just for evaluation - see `Switch`.
    """

    FAIL_MESSAGE: str = ""
    ERROR_CLS: Type[Exception] = ConditionNotMet
    INVERTED_COND_PREFIX: str = "Inverted condition: "
    DEFAULT_FORMAT_VALUE = "<NOT-PROVIDED>"

    def __init__(
        self,
        fail_message: Optional[str] = None,
        is_inverted: bool = False,
        error_cls: Optional[Type[Exception]] = None,
        name: Optional[str] = None,
    ):
        """
        :param fail_message: Message which is raised when condition is not met
        :param is_inverted: Whether condition is inverted
        :param error_cls: Exception class which will be raised if provided
        """
        self.fail_message = fail_message or self.FAIL_MESSAGE
        self.is_inverted = is_inverted
        self.error_cls = error_cls or self.ERROR_CLS
        super().__init__(description=self.fail_message, name=name)

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if self.validate(action_data):
            return action_data
        self.fail(action_data)

    def and_(self, condition: "Condition") -> ActionT:
        """
        Syntactic sugar for :func:`~orinoco.action.Action.then` - make more sense for conditions

        :param condition: Another condition which is evaluated after
        :return: :class:`~orinoco.action.ActionSet` of this condition and ``condition``
        """
        return self.then(condition)

    def fail(self, action_data: ActionDataT) -> NoReturn:
        """
        Method which is called when the condition is not met (and called by `Condition.run`)

        :param action_data: Action data for extracting custom information for the exception
        :return:
        """
        fail_message = self.fail_message
        fail_message_format_args = get_format_string_args(fail_message)
        if fail_message_format_args:
            format_dict = {
                k: action_data.get_by_key(k, default=self.DEFAULT_FORMAT_VALUE) for k in fail_message_format_args
            }
            print(format_dict)
            fail_message = fail_message.format(**format_dict)
        raise self.error_cls(
            "{}{} failed: {}".format("not " if self.is_inverted else "", self.__class__.__name__, fail_message)
        )

    def validate(self, action_data: ActionDataT) -> bool:
        """
        Main method which is used for evaluation

        :param action_data: Data container to be evaluated
        :return: Whether condition is met
        """
        val = self._is_valid(action_data)
        if self.is_inverted:
            return not val
        return val

    def validate_with(self, **params: Any) -> bool:
        """
        Shortcut for running validation without creating :class:`~orinoco.entities.ActionData` object directly

        :param params: Parameters which are propagated into action data
        :return: Whether condition is met
        """
        return self.validate(ActionData.create(**params))

    @abstractmethod
    def _is_valid(self, action_data: ActionDataT) -> bool:
        """
        Only abstract method which needs to be implemented by conditions. Please not that it's not intended
        for direct use since it's doesn't consider if the condition is inverted

        :param action_data: Data container to be evaluated
        :return: Whether condition is met
        """
        pass

    def __and__(self, other: "Condition") -> "AndOperator":
        """
        See :class:`AndOperator` docs. It provides and operator: `condition1 & condition2`

        :param other: Condition
        :return: :class:`AndOperator` instance (set of actions)
        """
        return AndOperator(self, other)

    def __or__(self, other: "Condition") -> "OrOperator":
        """
        See :class:`OrOperator` docs. It provides `or` operator: `condition1 | condition2`

        :param other: Second condition
        :return: :class:`OrOperator` instance (set of actions)
        """
        return OrOperator(self, other)

    def __invert__(self: ConditionT) -> ConditionT:
        return self.not_()

    def not_(self: ConditionT) -> ConditionT:
        """
        :return: Inverted condition
        """
        inv_cond = copy.deepcopy(self)
        inv_cond.is_inverted = not self.is_inverted
        inv_cond.fail_message = (
            "{}{}".format(self.INVERTED_COND_PREFIX, inv_cond.fail_message)
            if not inv_cond.fail_message.startswith(self.INVERTED_COND_PREFIX)
            else inv_cond.fail_message[len(self.INVERTED_COND_PREFIX) :]
        )
        inv_cond.description = inv_cond.fail_message
        return inv_cond

    def if_then(self, action: ActionT) -> "ConditionalAction":
        """
        Execute given action when this condition is valid

        :param action: Action to execute
        :return: :class:`ConditionalAction`
        """
        return ConditionalAction(self, action)

    @property
    def name_with_inverted(self) -> str:
        """
        :return: Name of the condition with inverted prefix if needed
        """
        return f'{"~" if self.is_inverted else ""}{self.name}'


class AlwaysTrue(Condition):
    """
    Dummy condition which is always true
    """

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return True


class GenericCondition(Condition):
    """
    Utility condition which allows creating conditions "on the fly" (usually by lambda functions)
    """

    def __init__(
        self,
        validation_method: Callable[[ActionDataT], bool],
        fail_message: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """
        :param validation_method: Method which returns whether the condition is met
        :param fail_message: Message which will be included in the raise exception
        """
        super().__init__(fail_message=fail_message, name=name)
        self.validation_method = validation_method

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return self.validation_method(action_data)


class PropertyCondition(Condition):
    """
    Generic condition which evaluates property of an object in the dataset
    """

    PROPERTY_OBJECT: Optional[str] = None
    ATTRIBUTE: Optional[str] = None
    EQUAL_TO: Optional[Any] = True

    SOMETHING: Any = object

    attribute: str

    def __init__(
        self,
        property_object: Optional[str] = None,
        attribute: Optional[str] = None,
        equal_to: Any = None,
        fail_message: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """
        Note that when parameters of constructor are not provided, class attributes are used. This could
        be convenient for subclassing without need to override constructor.

        :param property_object: Key of the object in the `ActionData`
        :param attribute: Attribute of the object to be evaluated
        :param equal_to: Expected value of the object's attribute
        :param fail_message:
        """
        super().__init__(
            fail_message=fail_message,
            name=name
            or "{}[{} {} {}]".format(self.__class__.__name__, attribute, "==" if equal_to else "!=", property_object),
        )
        self.attribute = attribute or self.ATTRIBUTE or raise_not_provided_field("attribute")
        self.equal_to = equal_to if equal_to is not None else self.EQUAL_TO
        self.property_object = property_object or self.PROPERTY_OBJECT or raise_not_provided_field("property_object")

    def _is_valid(self, action_data: ActionDataT) -> bool:
        value = getattr(action_data.get(self.property_object), self.attribute)
        if self.equal_to is self.SOMETHING:
            return bool(value)
        return value is self.equal_to


class ConditionSet(Condition, SystemActionTag):
    """
    Set of conditions
    """

    CONDITIONS: Optional[Iterable[Condition]] = None

    def __init__(
        self,
        conditions: Optional[Iterable[Condition]] = None,
        fail_message: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.conditions = conditions or self.CONDITIONS or raise_not_provided_field("conditions")

        super().__init__(
            fail_message=fail_message or "({})".format(", ".join([cond.fail_message for cond in self.conditions])),
            name="{}: {}".format(name or self.NAME, "AND ".join(cond.action_name for cond in self.conditions)),
        )

    @property
    def action_name(self) -> str:
        return "{}[{}]".format(
            self.name_with_inverted, "AND ".join(cond.name_with_inverted for cond in self.conditions)
        )

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return all(cond.validate(action_data) for cond in self.conditions)


class If(ConditionSet, SystemActionTag):
    """
    Syntactic sugar for `ConditionSet`. See `Switch` docs for usage.
    """

    def __init__(self, *conditions: Condition):
        super().__init__(conditions)


class Switch(Action, SystemActionTag):
    """
    Branching action which can be used in three ways:

    1) Create at once via constructor by pairs of condition-action
    2) By `if_then` chaining: Switch().if_then(cond1, action1).if_then(cond2, action2).otherwise(action3)
    3) Use `Switch.case` with `If` and `Then`:

    .. code-block:: python

        Switch()
        .case(
            If(cond.ClaimInValidState("PAID")),
            Then(
                ~cond.ClaimHasPaymentAuthorizationAssigned(
                    fail_message="You cannot reset a claim that has payment authorizations assigned!"
                ),
                cond.ClaimFromPaymentPlatform(),
                trans.StateToAuthorize(),
                trans.ResetEligibleAmount(),
            ),
        )
        .case(
            If(
                cond.ClaimInValidState("DECLINED", "CANCELLED"),
                cond.ClaimHasPreAuth(),
                ~cond.ClaimHasValidNotification(),
                ~cond.ClaimIsRetroactive(),
            ),
            Then(cond.UserCanResetClaim(), trans.StateToPendingAuthorization()),
        )
    """

    def __init__(
        self,
        paths: Optional[List[Tuple[Condition, Optional[ActionT]]]] = None,
        fail_message: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """
        :param paths: Pairs of condition and action which is executed whether condition is met
        :param fail_message:
        """
        super().__init__(description=fail_message, name=name)
        self.paths = paths or []

    @record_action
    @verbose_action_exception
    def run(self, action_data: ActionDataT) -> ActionDataT:
        for condition, action in self.paths:
            if action_data.skip_processing:
                return action_data

            action_data = action_data.record_start(condition)
            validation = condition.validate(action_data)
            action_data = action_data.record_end(condition)

            if validation:
                if not action:
                    return action_data
                return action.run(action_data)

        raise NoneOfActionsCanBeExecuted(
            "{}. {}".format(
                self.description, json.dumps([(p[0].action_name, p[0].description) for p in self.paths], indent=4)
            )
        )

    @async_record_action
    @async_verbose_action_exception
    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        for condition, action in self.paths:
            if action_data.skip_processing:
                return action_data

            action_data = action_data.record_start(condition)
            validation = condition.validate(action_data)
            action_data = action_data.record_end(condition)

            if validation:
                if not action:
                    return action_data
                return await action.async_run(action_data)

        raise NoneOfActionsCanBeExecuted(
            "{}. {}".format(
                self.description, json.dumps([(p[0].action_name, p[0].description) for p in self.paths], indent=4)
            )
        )

    def if_then(self, condition: Condition, action: Optional[ActionT] = None) -> "Switch":
        """
        Add a condition-action pair

        :param condition:
        :param action:
        :return: New `Switch` instance with appended condition-action pair
        """
        return Switch(self.paths + [(condition, action)], fail_message=self.description)

    def case(self, if_condition: If, then_action: Optional[Then] = None) -> "Switch":
        return self.if_then(if_condition, then_action)

    def otherwise(self, action: Optional[ActionT] = None) -> "Switch":
        return self.if_then(AlwaysTrue(), action)


class NonNoneDataValues(Condition):
    """
    Condition which determines whether certain values are not `None`
    """

    def __init__(self, *fields: str):
        """
        :param fields: Fields of :class:`~orinoco.entities.ActionData` which will be checked if are present and not none
        """
        super().__init__(
            fail_message="Data has to contain following non-none fields: {}".format(fields),
            name="{}[{}]".format(self.__class__.__name__, ", ".join(fields)),
        )
        self.fields = fields

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return all(action_data.is_in(field) and action_data.get(field) is not None for field in self.fields)


class BaseOperator(ConditionSet, ABC):
    """
    Base operator between two conditions
    """

    def __init__(self, cond1: Condition, cond2: Condition, description: Optional[str] = None):
        super().__init__(
            [cond1, cond2],
            fail_message=description or "Operator failed for {} and {}".format(cond1.action_name, cond2.action_name),
        )


class AndOperator(BaseOperator, SystemActionTag):
    def _is_valid(self, action_data: ActionDataT) -> bool:
        return all(cond.validate(action_data) for cond in self.conditions)


class OrOperator(BaseOperator, SystemActionTag):

    FAIL_MESSAGE = "None of conditions is True"

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return any(cond.validate(action_data) for cond in self.conditions)


class ConditionalAction(Action, SystemActionTag):
    """
    Represent action which is executed only when the given condition is met
    """

    def __init__(self, condition: Condition, action: ActionT, name: Optional[str] = None):
        super().__init__(name=name or "If({}) -> {}".format(condition.action_name, action.action_name))
        self.condition = condition
        self.action = action

    def run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if self.condition.validate(action_data):
            return self.action.run(action_data)
        return action_data

    async def async_run(self, action_data: ActionDataT) -> ActionDataT:
        if action_data.skip_processing:
            return action_data

        if self.condition.validate(action_data):
            return await self.action.async_run(action_data)
        return action_data


class IsInData(Condition):
    """
    Condition which checks whether a value with the given key exists in the data container
    """

    def __init__(self, field: str):
        super().__init__(
            fail_message="Field {} is not in data".format(field), name="{}[{}]".format(self.__class__.__name__, field)
        )
        self.field = field

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return action_data.is_in(self.field)


class SignatureIsInData(Generic[T], Condition):
    """
    Condition which checks whether a value with the given signature exists in the data container
    """

    def __init__(self, signature: Signature[T]):
        super().__init__(
            fail_message="Field {} is not in data".format(str(signature)),
            name="{}[{}]".format(self.__class__.__name__, signature),
        )
        self.signature = signature

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return action_data.signature_is_in(self.signature)


class LoopCondition(Condition, SystemActionTag):
    """
    Condition which checks whether values in an iterable item match the given condition
    """

    def __init__(
        self,
        method: Callable[[Union[List[bool], Generator[bool, None, None]]], bool],
        iterable_key: str,
        as_key: str,
        condition: Condition,
        fail_message: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.method = method
        self.iterable_key = iterable_key
        self.as_key = as_key
        self.condition = condition
        super().__init__(fail_message=fail_message, name=name)

    def _is_valid(self, action_data: ActionDataT) -> bool:
        return self.method(self._get_generator(action_data))

    def _get_generator(self, action_data: ActionDataT) -> Generator[bool, None, None]:
        return (
            self.condition.validate(action_data.evolve(**{self.as_key: value}))
            for value in action_data.get(self.iterable_key)
        )


class AnyCondition(LoopCondition, SystemActionTag):
    """
    Condition which checks whether any value in an iterable item match the given condition
    """

    def __init__(self, iterable_key: str, as_key: str, condition: Condition):
        super().__init__(
            method=any,
            iterable_key=iterable_key,
            as_key=as_key,
            condition=condition,
            fail_message="Any of the given items meet the condition [{}: {}]".format(
                condition.__class__.__name__, condition.fail_message
            ),
        )


class AllCondition(LoopCondition, SystemActionTag):
    """
    Condition which checks whether all values in an iterable item match the given condition
    """

    def __init__(self, iterable_key: str, as_key: str, condition: Condition):
        super().__init__(
            method=all,
            iterable_key=iterable_key,
            as_key=as_key,
            condition=condition,
            fail_message="All of the given items meet the condition [{}: {}]".format(
                condition.__class__.__name__, condition.fail_message
            ),
        )
