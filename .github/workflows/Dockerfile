FROM archlinux:latest
WORKDIR /
RUN pacman -Syu --noconfirm opencl-icd-loader
RUN pacman -S --noconfirm base-devel
RUN pacman -Syu --noconfirm go gcc git bzr jq pkg-config opencl-icd-loader opencl-headers
WORKDIR /lotus
COPY lotus lotus-storage-miner lotus-seal-worker .