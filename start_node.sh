#!/bin/bash

# Display usage information
usage() {
  echo "Usage: $0 [-i | -c | -d | -w | -e]"
  echo "  -i  Exclude INFO logs"
  echo "  -c  Exclude CRITICAL logs"
  echo "  -d  Exclude DEBUG logs"
  echo "  -w  Exclude WARNING logs"
  echo "  -e  Exclude ERROR logs"
  echo "You can combine flags to exclude multiple log levels."
  exit 1
}

# Default log filter (no exclusions)
log_filter="."

# Parse command-line options
while getopts ":icdwe" opt; do
  case ${opt} in
    i ) log_filter="${log_filter}\|INFO" ;;
    c ) log_filter="${log_filter}\|CRITICAL" ;;
    d ) log_filter="${log_filter}\|DEBUG" ;;
    w ) log_filter="${log_filter}\|WARNING" ;;
    e ) log_filter="${log_filter}\|ERROR" ;;
    \? ) usage ;;
  esac
done
shift $((OPTIND - 1))

# Process log filter
if [[ "$log_filter" != "." ]]; then
  log_filter=$(echo $log_filter | sed 's/^\|//')  # Remove leading "\|"
fi

# Start live tailing with filtering
if [[ -n "$log_filter" ]]; then
  tail -f node.log | grep -vE "$log_filter" --line-buffered
else
  tail -f node.log
fi