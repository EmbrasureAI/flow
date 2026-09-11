FROM rust:1.97.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev cmake pkg-config libssl-dev python3 && rm -rf /var/lib/apt/lists/*
ARG CARGO_BUILD_JOBS=2
ARG FLOW_FEATURES=""
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY vendor ./vendor
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git --mount=type=cache,target=/src/target cargo build --locked --release -p flow-daemon --features "$FLOW_FEATURES" && cp target/release/embrasure-flow /usr/local/bin/embrasure-flow
COPY LICENSE NOTICE ./
COPY licenses ./licenses
COPY scripts/package_licenses.py ./scripts/package_licenses.py
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git cargo fetch --locked && python3 scripts/package_licenses.py --features "$FLOW_FEATURES" --output /licenses

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 libstdc++6 && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home flow
COPY --from=build /usr/local/bin/embrasure-flow /usr/local/bin/embrasure-flow
COPY --from=build /licenses /usr/share/licenses/embrasure-flow
RUN mkdir /data && chown flow:flow /data
USER flow
WORKDIR /data
ENTRYPOINT ["embrasure-flow"]
CMD ["--config", "/etc/flow.toml", "run"]
