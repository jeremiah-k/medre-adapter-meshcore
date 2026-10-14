"""Pure adapter-owned destination-address validation."""

from medre.config.routes import RouteDestinationConfig


def validate_destination(destination: RouteDestinationConfig) -> None:
    """Reject addresses outside this transport's addressing contract."""
    if destination.destination_hash is None and destination.destination_name is None:
        raise ValueError(
            "destination_hash or destination_name is required for kind 'meshcore_contact'"
        )
