#! /usr/bin/env bash

set \
  -o errexit \
  -o pipefail

echo -e "\nWait for the initialization to complete before installing the recommended extensions.\n"

sudo apt update && sudo apt install -y \
  python-is-python3 \
  uuid-runtime

source "$HOME/.sdkman/bin/sdkman-init.sh" \
  && sdk env install

env $(cat .devcontainer/hadoop/config | grep -v '^#' | xargs) \
  .devcontainer/hadoop/scripts/envtoconf.py --destination $HADOOP_HOME/etc/hadoop

source "$HOME/.sdkman/bin/sdkman-init.sh" \
  && rm -rf .bsp .metals \
  && just clean \
  && scala setup-ide .

echo -e "\nInitialization completed. You can now install the recommended extensions.\n"
