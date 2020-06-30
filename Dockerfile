FROM archlinux:latest as builder
RUN pacman -Syu --noconfirm opencl-icd-loader
RUN pacman -S --noconfirm base-devel
RUN pacman -Syu --noconfirm go gcc git bzr jq pkg-config opencl-icd-loader opencl-headers
COPY . /lotus/
WORKDIR /lotus
RUN make clean && make all

# This file is a template, and might need editing before it works on your project.

FROM registry.cn-zhangjiakou.aliyuncs.com/wela/lotus:base
#将编译好的程序从打包镜像中复制到运行镜像中
COPY --from=builder /lotus/lotus   /usr/local/bin/
COPY --from=builder /lotus/lotus-storage-miner  /usr/local/bin/
COPY --from=builder /lotus/lotus-seal-worker /usr/local/bin/
WORKDIR /lotus
