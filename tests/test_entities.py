import pytest

from orinoco.entities import ActionConfig, Signature, ActionData
from orinoco.exceptions import AlreadyRegistered


def test_action_config() -> None:
    assert ActionConfig(
        INPUT={"x": Signature(type_=int, tags=set(), key="x"), "y": Signature(type_=int, tags=set(), key="y")},
        OUTPUT=Signature(type_=int, tags=set(), key="z"),
    ) == ActionConfig.create_strict(input_={"x": int, "y": int}, output_name="z", output_type=int)


def test_pattern_matching() -> None:
    action_data = ActionData(
        data=(
            (Signature(type_=str), "a"),
            (Signature(type_=str, tags={"xx"}), "b"),
            (Signature(type_=float, tags={"xx"}), "c"),
        )
    )

    assert {"a"} == set(action_data.find(Signature(type_=str, tags=set())))
    assert {"a", "b"} == set(action_data.find(Signature(type_=str)))
    assert {"c", "b"} == set(action_data.find(Signature(tags={"xx"})))
    assert {"c"} == set(action_data.find(Signature(type_=float)))


def test_already_registered() -> None:
    with pytest.raises(AlreadyRegistered):
        ActionData().register(Signature[int](key="bla"), entity=1).register(Signature[int](key="bla"), entity=2)

    ActionData().register(Signature[int](key="bla"), entity=1).register(
        Signature[int](key="bla"), entity=2, check_if_exists=False
    )


def test_action_data_created_from_keys() -> None:
    action_data = ActionData.create(age=1, first_name="Bruno")

    assert 1 == action_data.get_by_type(int) == action_data.get("age")
    assert "Bruno" == action_data.get_by_type(str) == action_data.get("first_name")


def test_dot_notation_nested_search() -> None:
    assert "chrome" == ActionData.create(request={"payload": {"meta": {"browser": "chrome"}}}).get(
        "request.payload.meta.browser"
    )
