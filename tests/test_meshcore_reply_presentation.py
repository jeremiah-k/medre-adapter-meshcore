"""Bridge-address cleanup preserves evidence without fabricating reply targets."""

import pytest
from medre.adapters.meshcore.codec import MeshCoreCodec
from medre.adapters.meshcore.attribution import project_meshcore_attribution
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.rendering.attribution import (
    build_relay_attribution,
    format_relay_prefix,
)


@pytest.mark.parametrize("address", ["@[MEDRE-MC-A] ", "@[MEDRE-MC-A]: "])
def test_own_address_removed_without_inventing_reply(address):
    """Automatic bridge mentions normalize display only; raw text and ID remain stable."""
    codec = MeshCoreCodec("meshcore", MeshCoreConfig(adapter_id="meshcore"))
    packet = {
        "type": "CHAN",
        "channel_idx": 3,
        "sender_timestamp": 42,
        "text": "MEDRE-MC-B: " + address + "Not bad 😎",
        "txt_type": 0,
    }
    untouched = codec.decode(packet)
    event = codec.decode(packet, local_node_name="MEDRE-MC-A")
    assert event.payload["body"] == "Not bad 😎"
    assert not event.relations
    assert event.source_native_ref == untouched.source_native_ref
    assert event.metadata.native.data["meshcore"]["wire_text"] == packet["text"]


@pytest.mark.parametrize(
    "body,local",
    [
        ("@[Other] hello", "MEDRE-MC-A"),
        ("@[MEDRE-MC-A]hello", "MEDRE-MC-A"),
        ("@[MEDRE-MC-A] ", "MEDRE-MC-A"),
        ("@[MEDRE-MC-A] hello", None),
    ],
)
def test_other_or_unknown_mentions_are_preserved(body, local):
    """Only a nonempty leading automatic address to the known local bridge is removed."""
    codec = MeshCoreCodec("meshcore", MeshCoreConfig(adapter_id="meshcore"))
    event = codec.decode(
        {
            "type": "CHAN",
            "channel_idx": 3,
            "sender_timestamp": 42,
            "text": "MEDRE-MC-B: " + body,
            "txt_type": 0,
        },
        local_node_name=local,
    )
    assert event.payload["body"] == body


def test_prefix_can_hide_annotation_without_changing_verification():
    """Operators may choose raw names; structured provenance remains unverified."""
    codec = MeshCoreCodec("meshcore", MeshCoreConfig(adapter_id="meshcore"))
    event = codec.decode(
        {
            "type": "CHAN",
            "channel_idx": 3,
            "sender_timestamp": 42,
            "text": "MEDRE-MC-B: hello",
            "txt_type": 0,
        }
    )
    projected = project_meshcore_attribution(dict(event.metadata.native.data))
    attr = build_relay_attribution(event, projected_fields=projected)
    assert (
        format_relay_prefix("{sender}", attr).rendered_prefix
        == "MEDRE-MC-B (unverified)"
    )
    assert format_relay_prefix("{sender_raw}", attr).rendered_prefix == "MEDRE-MC-B"
    assert attr.source_sender_verification == "unverified"


async def test_adapter_supplies_own_node_name_to_codec(
    make_adapter_context, inbound_collector
):
    """The production adapter supplies its own name for reply-address cleanup."""
    from medre.adapters.meshcore.adapter import MeshCoreAdapter

    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="meshcore", connection_type="fake", identity="MEDRE-MC-A"
        )
    )
    await adapter.start(make_adapter_context("meshcore"))
    try:
        await adapter.simulate_inbound(
            {
                "type": "CHAN",
                "channel_idx": 3,
                "sender_timestamp": 42,
                "text": "MEDRE-MC-B: @[MEDRE-MC-A] Not bad 😎",
                "txt_type": 0,
            }
        )
        assert len(inbound_collector.events) == 1
        assert inbound_collector.events[0].payload["body"] == "Not bad 😎"
        assert not inbound_collector.events[0].relations
    finally:
        await adapter.stop()
