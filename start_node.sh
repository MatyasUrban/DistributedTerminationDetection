#!/bin/bash

# Function to display usage
usage() {
  echo "Usage: $0 -s severity"
  echo "  -s severity  Specify the logging severity level (warning/critical)"
  exit 1
}

# Default values
severity="critical"

# Parse command-line arguments
while getopts ":s:" opt; do
  case ${opt} in
    s )
      severity=$OPTARG
      ;;
    \? )
      echo "Invalid Option: -$OPTARG" 1>&2
      usage
      ;;
    : )
      echo "Invalid Option: -$OPTARG requires an argument" 1>&2
      usage
      ;;
  esac
done
shift $((OPTIND -1))

# Validate severity
if [[ "$severity" != "warning" && "$severity" != "critical" ]]; then
    echo "Error: Invalid severity level. Use 'warning' or 'critical'."
    usage
fi

# Function to clean up screen sessions
cleanup_sessions() {
  # Kill 'node' session if it exists
  if screen -list | grep -q "node"; then
    screen -S node -X quit
    echo "Screen session 'node' terminated."
  fi

  # Kill 'logs' session if it exists
  if screen -list | grep -q "logs"; then
    screen -S logs -X quit
    echo "Screen session 'logs' terminated."
  fi
}

# Check if the 'node' screen session already exists
if screen -list | grep -q "node"; then
  echo "Screen session 'node' already exists."
else
  # Start the node.py script directly in a new screen session
  screen -dmS node bash -c "python3 node.py -s $severity; $(cleanup_sessions)"
  echo "Screen session 'node' started running node.py with severity '$severity'."
fi

# Check if the 'logs' screen session already exists
if screen -list | grep -q "logs"; then
  echo "Screen session 'logs' already exists."
else
  # Start a screen session to tail the log file
  screen -dmS logs bash -c "tail -f node.log"
  echo "Screen session 'logs' started to display node.log."
fi

# Attach to the 'node' screen session
screen -r node
