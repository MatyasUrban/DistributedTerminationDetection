#!/bin/bash

# Parse arguments
usage() {
  echo "Usage: $0 -s severity"
  echo "  -s severity  Specify the logging severity level (warning/critical)"
  exit 1
}

severity="critical"
while getopts ":s:" opt; do
  case ${opt} in
    s ) severity=$OPTARG ;;
    \? | : ) usage ;;
  esac
done
shift $((OPTIND -1))

[[ "$severity" != "warning" && "$severity" != "critical" ]] && usage

# Start logs session
screen -list | grep -q "logs" || screen -dmS logs bash -c "tail -f node.log"

# Start node session
if screen -list | grep -q "node"; then
  echo "Screen session 'node' already exists."
else
  screen -dmS node bash -c "python3 node.py -s $severity; screen -S logs -X quit; screen -S node -X quit"
  echo "Screen session 'node' started running node.py with severity '$severity'."
fi

screen -r node