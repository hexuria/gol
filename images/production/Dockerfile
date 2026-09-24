FROM ubuntu:24.04

# Production agent computer.
# The desktop chooses Box and the server starts this image.
# Local is images/local; the desktop starts that image with Docker.
# This container does not call the inference proxy and holds no vendor tokens.
#
# Agent contract (same entrypoint as the local image): a volume at
# /workspace, sleep until docker exec, a minimal XFCE, and AGENT=rust adds
# the Rust toolchain. Placement changes. The contract does not.

LABEL org.gol.placement="box"

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        xfce4-session \
        xfce4-panel \
        xfce4-terminal \
        xfdesktop4 \
        xfwm4 \
        dbus-x11 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG AGENT=base
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH="/usr/local/cargo/bin:${PATH}"
# The Rust-building variant installs the toolchain. The base image does not.
RUN if [ "$AGENT" = "rust" ]; then \
        apt-get update && apt-get install -y --no-install-recommends curl gcc libc6-dev && \
        curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | \
            sh -s -- -y --default-toolchain 1.98.1 --profile minimal --no-modify-path && \
        chmod -R a+rX /usr/local/rustup /usr/local/cargo && \
        rm -rf /var/lib/apt/lists/*; \
    fi

COPY agent-entrypoint.sh /usr/local/bin/gol-agent
RUN chmod 755 /usr/local/bin/gol-agent \
    && useradd --create-home --shell /bin/bash agent \
    && mkdir -p /workspace \
    && chown agent:agent /workspace
WORKDIR /workspace
VOLUME /workspace
USER agent
ENTRYPOINT ["/usr/local/bin/gol-agent"]
