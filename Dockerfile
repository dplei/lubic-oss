# 总控与 agent 同一个镜像，启动命令选跑哪个（见部署文档）。
# 基础镜像按 digest 锁定，从哪个镜像站拉内容都照样校验。
# 升级 Rust 时同步改 CI 里固定的 Rust 版本（如有）。
ARG RUST_IMAGE=rust:1.96-trixie@sha256:1f0dbad1df66647807e6952d1db85d0b2bda7606cb2139d82517e4f009967376
ARG RUNTIME_IMAGE=debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad

FROM ${RUST_IMAGE} AS rust

# 本地 `docker build .` 走这里：先测再编。CI 在 runner 上编好二进制，用 `--build-context bin=<目录>`
# 顶替下面的 bin 阶段，这一段不会执行。
FROM rust AS build
WORKDIR /src
COPY . .
# 提交号编进程序（`--version`、agent 心跳、`/v1/version`）；本地构建不传时为空。
# GitHub push 事件的时间；手动发布或本地构建没有时留空。
ARG GIT_COMMIT=""
ARG GIT_PUSHED_AT=""
# 测试用 release profile，依赖与下面的构建共用一次编译。
RUN cargo test --workspace --release --locked \
    && cargo build --release --locked -p lubic-controller -p lubic-agent

FROM scratch AS bin
COPY --from=build /src/target/release/lubic-controller /src/target/release/lubic-agent /

FROM ${RUNTIME_IMAGE}
# 更新脚本按这个 label 读镜像的提交号。
ARG GIT_COMMIT=""
LABEL org.opencontainers.image.revision=${GIT_COMMIT}
# 运行时只需 CA 证书和时区，从构建镜像拷，不在运行镜像里装包。
COPY --from=rust /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=rust /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=bin /lubic-controller /lubic-agent /usr/local/bin/
WORKDIR /data
CMD ["lubic-agent", "-c", "/data/agent.toml"]
