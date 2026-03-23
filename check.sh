#!/bin/bash

set -e

RED="\e[31m"
GREEN="\e[32m"
NC="\e[0m"

fail() {
    echo -e "${RED}$1${NC}"
    exit 1
}

check_tool() {
    local cmd=$1
    local version_cmd=$2

    echo -n "Checking ${cmd}... "

    if ! command -v "$cmd" >/dev/null 2>&1; then
        fail "${cmd} not found"
    fi

    if ! $version_cmd >/dev/null 2>&1; then
        fail "${cmd} version check failed"
    fi

    echo -e "${GREEN}OK${NC}"
    $version_cmd
    echo "----------------------------------------"
}

check_tool "kind" "kind --version"
check_tool "kubectl" "kubectl version --client=true"
check_tool "helm" "helm version"
check_tool "docker" "docker --version"
check_tool "helm" "helm diff version"

echo -e "${GREEN}All tools are installed and working.${NC}"
