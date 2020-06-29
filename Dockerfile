FROM archlinux:latest as builder
WORKDIR /
RUN pacman -Syu --noconfirm opencl-icd-loader
RUN pacman -S --noconfirm base-devel
RUN pacman -Syu --noconfirm go gcc git bzr jq pkg-config opencl-icd-loader opencl-headers
COPY .github/workflows .
RUN make clean && make all
# This file is a template, and might need editing before it works on your project.

FROM archlinux:latest
WORKDIR /lotus
#将编译好的程序从打包镜像中复制到运行镜像中
COPY --from=builder lotus .
COPY --from=builder lotus-storage-miner .
COPY --from=builder lotus-seal-worker .