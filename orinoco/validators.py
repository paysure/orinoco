from functools import wraps
from typing import Callable, TypeVar, Protocol, Optional, Type, Set, cast

from typing_extensions import ParamSpec, Concatenate

from orinoco.exceptions import ActionNotProperlyConfigured
from orinoco.types import ConfigurableActionT, ActionConfigT

OutputT = TypeVar("OutputT")
P = ParamSpec("P")


class ConfigurableActionP(Protocol):
    config: ActionConfigT


class SetSignatureT(Protocol):
    type_: Optional[Type]
    tags: Set[str]
    key: str


class ActionConfigWithOutputT:
    # INPUT: Optional[Dict[str, SignatureT[Any]]]
    OUTPUT: SetSignatureT


class _ConfigurableActionWithOutputT(Protocol):
    config: ActionConfigWithOutputT


def check_output_key_configured(
    fu: Callable[Concatenate[_ConfigurableActionWithOutputT, P], OutputT]
) -> Callable[Concatenate[_ConfigurableActionWithOutputT, P], OutputT]:
    @wraps(fu)
    def wrapper(configurable_action: _ConfigurableActionWithOutputT, *args: P.args, **kwargs: P.kwargs) -> OutputT:
        if configurable_action.config.OUTPUT is None or configurable_action.config.OUTPUT.key is None:
            raise ActionNotProperlyConfigured("Retry until condition has to be annotated with the name of the output")
        return fu(configurable_action, *args, **kwargs)

    return wrapper
