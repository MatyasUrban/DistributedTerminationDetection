import logging
import argparse

# Argument parsing for severity level
parser = argparse.ArgumentParser(description="Node script to log user inputs.")
parser.add_argument(
    "-s", "--severity", choices=["warning", "critical"], default="critical",
    help="Set the severity level for logging (default: critical)"
)
args = parser.parse_args()

# Configure logging
log_file = "node.log"
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

# Severity mapping
severity_map = {
    "warning": logging.WARNING,
    "critical": logging.CRITICAL
}
log_severity = severity_map[args.severity]

def main():
    print(f"Node is running. Logging severity: {args.severity.upper()}. Type 'exit' to quit.")
    while True:
        try:
            user_input = input("Enter command: ")
            if user_input.lower() == "exit":
                print("Shutting down node.")
                logging.log(log_severity, "Node shutting down.")
                break
            logging.log(log_severity, f"User input: {user_input}")
            print(f"Logged: {user_input}")
        except KeyboardInterrupt:
            print("\nShutting down node due to keyboard interrupt.")
            logging.log(log_severity, "Node shutting down due to keyboard interrupt.")
            break

if __name__ == "__main__":
    main()
