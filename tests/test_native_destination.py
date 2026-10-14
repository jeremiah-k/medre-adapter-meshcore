"""Native address rules are owned and enforced by the installed adapter."""

import pytest
from medre.adapter_registry import get_destination_spec
from medre.config.errors import ConfigValidationError
from medre.config.routes import RouteDestinationConfig


def test_registered_native_destination_validates() -> None:
    assert get_destination_spec("meshcore_contact") is not None
    destination = RouteDestinationConfig.from_dict(
        "native",
        {"kind": "meshcore_contact", **{"destination_name": "contact"}},
        field_name="dest_destination",
        section_path="routes.native",
    )
    assert destination.kind == "meshcore_contact"


@pytest.mark.parametrize(
    "address", [{}, {"destination_hash": None, "destination_name": None}]
)
def test_native_destination_rejects_invalid_address(address: dict) -> None:
    with pytest.raises(ConfigValidationError):
        RouteDestinationConfig.from_dict(
            "native",
            {"kind": "meshcore_contact", **address},
            field_name="dest_destination",
            section_path="routes.native",
        )
