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

# Start Python process in a new screen session
screen -S python-session -dm bash -c "python3 node.py; read -p 'Python process ended. Press Enter to close...'"

# Start log-tailing process in another screen session
screen -S logs-session -dm bash -c "tail -f node.log | grep $log_filter --line-buffered; read -p 'Log tailing ended. Press Enter to close...'"

# Arrange the sessions side by side horizontally
screen -S python-session -X split -h
screen -S python-session -X screen -t logs-session screen -r logs-session

# Reattach to the Python session
screen -r python-session