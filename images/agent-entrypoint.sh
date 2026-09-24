#!/bin/sh
# Agent contract, shared by the local and production images.
# Stay up so the host can docker exec commands into /workspace.
# This process does not call the inference proxy and does not read vendor tokens.
exec sleep infinity
