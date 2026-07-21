#!/usr/bin/env bash

# Wrapper script calling real kafka topic creator
exec "$(dirname "$0")/../infra/kafka/create-topics.sh" "$@"
