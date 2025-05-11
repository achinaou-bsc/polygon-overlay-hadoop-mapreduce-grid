FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

SHELL ["/bin/bash", "-c"]

RUN apt update && apt install -y \
  curl \
  zip \
  unzip \
  just

USER vscode

RUN curl -s "https://get.sdkman.io" | bash
