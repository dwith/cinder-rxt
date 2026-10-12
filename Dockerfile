ARG VERSION=master-ubuntu_jammy

# Stage 1: Build Rust snapshot worker
FROM rust:1-bookworm AS rust-builder
WORKDIR /usr/src/cinder-rxt
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# Stage 2: Build Cinder Python driver package
FROM openstackhelm/cinder:${VERSION} AS python-builder
RUN apt update && apt install -y git
RUN /var/lib/openstack/bin/pip install --upgrade --force-reinstall pip
WORKDIR /opt/cinder-rxt
COPY . /opt/cinder-rxt
RUN /var/lib/openstack/bin/pip install --no-cache-dir /opt/cinder-rxt
RUN find /var/lib/openstack -regex '^.*\(__pycache__\|\.py[co]\)$' -delete

# Stage 3: Final Cinder image
FROM openstackhelm/cinder:${VERSION}
COPY --from=rust-builder /usr/src/cinder-rxt/target/release/cinder-rxt-worker /usr/local/bin/cinder-rxt-worker
RUN chmod +x /usr/local/bin/cinder-rxt-worker
COPY --from=python-builder /var/lib/openstack/. /var/lib/openstack/

