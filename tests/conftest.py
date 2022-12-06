import pytest

from orinoco import config


@pytest.fixture
def with_strict_mode():
    current_mode = config.IMPLICIT_TYPE_STRICT_MODE_ENABLED
    config.IMPLICIT_TYPE_STRICT_MODE_ENABLED = True
    yield
    config.IMPLICIT_TYPE_STRICT_MODE_ENABLED = current_mode
