FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p playvortex-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/playvortex-server /usr/local/bin/playvortex-server
ENV PLAYVORTEX_BIND=0.0.0.0:3000
ENV PLAYVORTEX_DATABASE=/data/playvortex.sqlite3
VOLUME /data
EXPOSE 3000
CMD ["playvortex-server"]
