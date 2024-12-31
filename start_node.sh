#!/bin/bash

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

log_filter="."

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

# Remove leading "\|" if filters are set
if [[ "$log_filter" != "." ]]; then
  log_filter=$(echo $log_filter | sed 's/^\|//')
  log_filter="-vE \"$log_filter\""  # Use `grep -vE` for exclusion
else
  log_filter=""
fi

screen -S node-session -dm bash -c "python3 node.py; screen -S node-session -X quit"
screen -S node-session -X screen bash -c "tail -f node.log | grep $log_filter --line-buffered; screen -S node-session -X quit"
screen -S node-session -X split -v
screen -S node-session -X focus
screen -S node-session -X select 0
screen -r node-session
