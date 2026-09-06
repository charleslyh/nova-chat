"""L4: drive the compatibility layer with the official OpenAI Python SDK.

The whole point of D27 is that an unmodified official client can drive the
conversation endpoints. This script is the executable form of that claim: it does
nothing but call `client.conversations.*` and `client.responses.create` against a
running gateway and assert the results parse and round-trip.

Skipping (missing Python / missing package) is the runner's job, not this script's:
if the `openai` package is importable, every assertion here must hold.

Run inside the L2 fixture (mem-server + agentd + gateway on 127.0.0.1:18080), so
a `responses.create` actually completes against the scripted mock model.
"""

import os
import sys

from openai import OpenAI

GATEWAY = os.environ.get("NOVA_GATEWAY_URL", "http://127.0.0.1:18080/v1")


def fail(message: str) -> int:
    print(f"L4 SDK-COMPAT FAIL: {message}", file=sys.stderr)
    return 1


def main() -> int:
    # A dummy key: the fixture runs in local (unauthenticated) mode, and the SDK
    # always sends a key, so it must be a value the local table accepts.
    client = OpenAI(base_url=GATEWAY, api_key="sk-l4-fixture-key-0000")

    # 1. Create the conversation the official way.
    conv = client.conversations.create(metadata={"topic": "sdk-compat"})
    conv_id = conv.id
    if not conv_id.startswith("conv_"):
        return fail(f"unexpected conversation id {conv_id!r}")

    # 2. A response started through the conversation pointer, synchronously. The
    #    scripted model completes, so this returns a terminal response object.
    resp = client.responses.create(
        model="m", input="sdk-marker", conversation=conv_id
    )
    if not resp.id.startswith("resp_"):
        return fail(f"unexpected response id {resp.id!r}")
    if resp.status != "completed":
        return fail(f"expected completed response, got {resp.status!r}")

    # 3. Retrieve the conversation and update its metadata, both the official way.
    fetched = client.conversations.retrieve(conv_id)
    if fetched.id != conv_id:
        return fail(f"retrieve mismatch: {fetched.id!r} != {conv_id!r}")

    updated = client.conversations.update(
        conv_id, metadata={"topic": "sdk-compat-updated"}
    )
    if updated.metadata.get("topic") != "sdk-compat-updated":
        return fail(f"metadata update did not round-trip: {updated.metadata!r}")

    # 4. The second turn through the same pointer inherits the first (FR-41 is
    #    exercised at the HTTP level by the L2 scenario; here we only assert the
    #    SDK can drive it and the server accepts the continuation).
    second = client.responses.create(
        model="m", input="sdk-marker-again", conversation=conv_id
    )
    if second.status != "completed":
        return fail(f"second turn did not complete: {second.status!r}")

    # 5. Delete the conversation the official way.
    deleted = client.conversations.delete(conv_id)
    if getattr(deleted, "deleted", None) is not True:
        return fail("conversation deletion did not report deleted=true")

    print("l4 sdk-compat OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
