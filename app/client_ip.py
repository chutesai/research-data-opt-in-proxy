from __future__ import annotations

from fastapi import Request


def extract_client_ip(request: Request) -> str | None:
    """Return the caller IP using Vercel-managed headers when present.

    This service is deployed directly on Vercel. Their edge sets
    ``x-forwarded-for`` to the caller's public IP and overwrites that header to
    prevent spoofing when Vercel sits behind another proxy. We intentionally do
    not trust caller-supplied ``x-real-ip`` here.
    """

    for header_name in (
        "x-forwarded-for",
        "x-vercel-forwarded-for",
    ):
        header_value = request.headers.get(header_name)
        if header_value:
            return header_value.split(",")[0].strip()

    if request.client:
        return request.client.host
    return None
