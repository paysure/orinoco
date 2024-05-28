import asyncio
from dataclasses import dataclass
from typing import Optional, Generator

import pytest
from typing_extensions import Annotated

from orinoco.data_source import DataSource
from orinoco.entities import ActionData, ActionConfig, Signature, NothingFound
from orinoco.exceptions import FoundMoreThanOne, ConditionNotMet, ActionNotProperlyConfigured
from orinoco.transformation import Transformation
from orinoco.typed_action import TypedAction, AsyncTypedAction, TypedCondition
from orinoco.types import ActionDataT


def test_implicit_actions_config() -> None:
    class MyAction(TypedAction[str]):
        def __call__(self, length: float, is_metric: bool) -> str:
            return "ok: {} {}".format(length, "cm" if is_metric else "inch")

    action_data = MyAction().run_with_data(length=1.2, is_metric=False, x=1)

    assert "ok: 1.2 inch" == action_data.get_by_type(str)


def test_implicit_actions_config_with_annotated() -> None:
    class MyAction(TypedAction[str]):
        def __call__(self, length: float, is_metric: bool) -> Annotated[str, "my_value"]:
            return "ok: {} {}".format(length, "cm" if is_metric else "inch")

    action_data = MyAction().run_with_data(length=1.2, is_metric=False, x=1)

    assert "ok: 1.2 inch" == action_data.get("my_value")


def test_implicit_actions_config_with_annotated_with_tags() -> None:
    class MyAction(TypedAction[str]):
        def __call__(self, length: float, is_metric: bool) -> Annotated[str, "my_value", "my_tag1", "my_tag2"]:
            return "ok: {} {}".format(length, "cm" if is_metric else "inch")

    action_data = MyAction().run_with_data(length=1.2, is_metric=False, x=1)

    assert (
        "ok: 1.2 inch"
        == action_data.find_one(Signature(tags={"my_tag1"}))
        == action_data.find_one(Signature(tags={"my_tag2"}))
        == action_data.find_one(Signature(tags={"my_tag1", "my_tag2"}))
        == action_data.get_by_tags("my_tag1")
        == action_data.get_by_tags("my_tag1", "my_tag2")
    )

    with pytest.raises(NothingFound):
        action_data.find_one(Signature(tags={"non_existing_tag"}))

    with pytest.raises(NothingFound):
        action_data.get_by_tags("my_tag1", "my_tag2", "non_existing_tag")


def test_implicit_actions_config_should_fail_in_strict_mode(with_strict_mode) -> None:
    class MyAction(TypedAction[str]):
        def __call__(self, length: float, is_metric: bool) -> str:
            return "ok: {} {}".format(length, "cm" if is_metric else "inch")

    with pytest.raises(ActionNotProperlyConfigured):
        MyAction().run_with_data(length=1.2, is_metric=False, x=1)


def test_explicit_action_many_of_one_type() -> None:
    class DoubleValue(TypedAction[float]):
        CONFIG = ActionConfig(
            INPUT=({"value": Signature(type_=float)}), OUTPUT=Signature(key="doubled_value", type_=float)
        )

        def __call__(self, value: float) -> float:
            return 2 * value

    action_data = ActionData.create(x=1.2)

    assert 1.2 == action_data.get_by_type(float) == action_data.get("x")

    result_action_data = DoubleValue().run(action_data)

    with pytest.raises(FoundMoreThanOne):
        result_action_data.get_by_type(float)

    assert 2.4 == result_action_data.get("doubled_value")
    assert 1.2 == result_action_data.get("x")


def test_explicit_action_with_already_existing_name() -> None:
    class DoubleValue(TypedAction[float]):
        CONFIG = ActionConfig(
            INPUT=({"value": Signature(type_=float)}), OUTPUT=Signature(key="doubled_value", type_=float)
        )

        def __call__(self, value: float) -> float:
            return 2 * value

    action_data = DoubleValue().run_with_data(value=1.2, doubled_value=10)
    assert 2.4 == action_data.get("doubled_value")


def test_explicit_actions_with_tags() -> None:
    @dataclass
    class Payment:
        amount: float

    class MergeFirstTwoPayments(TypedAction[Payment]):
        CONFIG = ActionConfig(
            INPUT={
                "first": Signature(type_=Payment, tags={"first_payment"}),
                "second": Signature(type_=Payment, tags={"second_payment"}),
            },
            OUTPUT=Signature(type_=Payment, tags={"combined_payment"}),
        )

        def __call__(self, first: Payment, second: Payment) -> Payment:
            return Payment(amount=first.amount + second.amount)

    action_data = MergeFirstTwoPayments().run(
        ActionData(
            data=[
                (Signature(type_=Payment, tags={"original_payment", "first_payment"}), Payment(amount=12)),
                (Signature(type_=Payment, tags={"second_payment"}), Payment(amount=3)),
                (Signature(type_=Payment, tags={"third_payment"}), Payment(amount=4)),
                (Signature(type_=Payment), Payment(amount=5)),
            ]
        )
    )
    assert 15 == action_data.get_by_signature(Signature[Payment](type_=Payment, tags={"combined_payment"})).amount
    assert (
        12
        == action_data.find_one(Signature[Payment](type_=Payment, tags={"original_payment"})).amount
        == action_data.find_one(Signature[Payment](type_=Payment, tags={"first_payment"})).amount
        == action_data.get_by_signature(
            Signature[Payment](type_=Payment, tags={"original_payment", "first_payment"})
        ).amount
    )

    assert [Payment(12), Payment(3), Payment(4), Payment(5), Payment(15)] == action_data.find(Signature(type_=Payment))


def test_chaining_with_other_actions() -> None:
    @dataclass
    class User:
        name: str

    class CreateUser(DataSource):
        PROVIDES = "user"

        def get_data(self, action_data: ActionDataT) -> User:
            return User(name=action_data.get("username"))

    class SendEmail(TypedAction[None]):
        sent_emails_to = []

        def __call__(self, user: User) -> None:
            self.sent_emails_to.append(user)

    class IsSuperUser(TypedAction[bool]):
        CONFIG = ActionConfig(
            INPUT={"user": Signature(type_=User)}, OUTPUT=Signature(key="is_super_user", type_=bool)
        )

        def __call__(self, user: User) -> bool:
            return user.name == "admin"

    class AddSuperUserSuffixName(Transformation):
        def transform(self, action_data: ActionDataT) -> ActionDataT:
            user = action_data.get("user")
            return action_data.evolve(
                user=User(name=user.name + "__superuser") if action_data.get("is_super_user") else user
            )

    action = CreateUser() >> IsSuperUser() >> AddSuperUserSuffixName() >> SendEmail()

    action_data1 = action.run_with_data(username="admin")
    action_data2 = action.run_with_data(username="Karel")

    assert action_data1 == ActionData(
        data=[
            (Signature(type_=str, tags=set(), key="username"), "admin"),
            (Signature(type_=bool, tags=set(), key="is_super_user"), True),
            (Signature(type_=User, tags=set(), key="user"), User(name="admin__superuser")),
        ],
        futures=[],
        observers=action_data1.observers,
    )

    assert action_data2 == ActionData(
        data=[
            (Signature(type_=str, tags=set(), key="username"), "Karel"),
            (Signature(type_=bool, tags=set(), key="is_super_user"), False),
            (Signature(type_=User, tags=set(), key="user"), User(name="Karel")),
        ],
        futures=[],
        observers=action_data2.observers,
    )

    assert action_data1.get_by_type(User) == User("admin__superuser")
    assert action_data2.get_by_type(User) == User("Karel")

    assert SendEmail.sent_emails_to == [User(name="admin__superuser"), User(name="Karel")]


def test_async_action() -> None:
    class SyncIntSum(TypedAction[int]):
        def __call__(self, x: float, y: float) -> int:
            return int(x + y)

    class AsyncIntSum(AsyncTypedAction[int]):
        SYNC_ACTION = SyncIntSum

        async def __call__(self, x: float, y: float) -> int:
            return int(x + y)

    async def run_pipelines():
        return await asyncio.gather(
            AsyncIntSum().async_run_with_data(x=1.0, y=3.0), SyncIntSum().async_run_with_data(x=1.0, y=3.0)
        )

    action_data_async, action_data_sync = asyncio.run(run_pipelines())

    action_data_async_sync_version = AsyncIntSum().run_with_data(x=1.0, y=3.0)

    assert (
        4
        == action_data_async.get_by_type(int)
        == action_data_sync.get_by_type(int)
        == action_data_async_sync_version.get_by_type(int)
    )


def test_implicit_condition() -> None:
    class IsPositive(TypedCondition):
        def __call__(self, value: float) -> bool:
            return value >= 0

    _test_condition(IsPositive())


def test_explicit_condition() -> None:
    class IsPositive(TypedCondition):
        CONFIG = ActionConfig(INPUT={"value": Signature(key="value")})

        def __call__(self, value: float) -> bool:
            return value >= 0

    _test_condition(IsPositive())


def test_as_output(double_typed_action: TypedAction) -> None:
    class MyNumber(int):
        pass

    result = double_typed_action.output_as(key="x_doubled", type_=MyNumber).run_with_data(x=3)

    assert {"x", "x_doubled"} == {k.key for k, v in result.data}
    assert result.get("x_doubled") == result.get_by_type(MyNumber) == 6


def _test_condition(is_positive: TypedCondition) -> None:

    assert is_positive.validate(ActionData.create(value=1.1))
    assert not is_positive.validate(ActionData.create(value=-3.3))

    # Check that condition validation result is not added
    assert is_positive.run_with_data(value=2.1).signatures == [Signature(type_=float, tags=set(), key="value")]

    with pytest.raises(ConditionNotMet):
        is_positive.run_with_data(value=-10)


def test_as_input(double_typed_action: TypedAction) -> None:
    class MyNumber(int):
        pass

    result = double_typed_action.input_as(x="different_input").run_with_data(different_input=MyNumber(3))

    assert result.get("double") == 6


def test_default_inputs():
    class WithDefaultAdd(TypedAction):
        def __call__(self, x: int, y: int = 1) -> Annotated[int, "sum"]:
            return x + y

    assert WithDefaultAdd().run_with_data(x=1).get_by_key("sum") == 2
    assert WithDefaultAdd().run_with_data(x=1, y=4).get_by_key("sum") == 5


def test_optional_output():
    class OptionalOutput(TypedAction):
        def __call__(self, x: int) -> Annotated[Optional[int], "sum"]:
            return x if x > 0 else None

    assert OptionalOutput().run_with_data(x=1).get_by_key("sum") == 1
    assert OptionalOutput().run_with_data(x=-1).get_by_key("sum") is None


def test_generator_action():
    class GeneratorAction(TypedAction):
        def __call__(self, x: int) -> Annotated[Generator[int, None, None], "my_gen"]:
            yield x
            yield x + 1

    gen = GeneratorAction().run_with_data(x=1).get("my_gen")
    assert isinstance(gen, Generator)
    assert next(gen) == 1
    assert next(gen) == 2
