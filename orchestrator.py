import subprocess
import time
import sys
import os

DRAIN_TIME = 15
PAUSE_BETWEEN = 5

def kill_all_python():
    """Kill any leftover python processes except ourselves"""
    our_pid = os.getpid()
    subprocess.run(
        f'wmic process where "name=\'python.exe\' and processid!=\'{our_pid}\'" call terminate',
        shell=True, capture_output=True
    )
    time.sleep(2)

def run_cycle():
    print(f"\n{'='*50}")
    print(f"Starting new replay cycle at {time.strftime('%H:%M:%S')}")
    print(f"{'='*50}\n")

    feed_handler = subprocess.Popen(
        [sys.executable, "feed_handler.py"],
        cwd=os.path.dirname(os.path.abspath(__file__)) or ".",
    )

    time.sleep(3)

    broadcaster = subprocess.Popen(
        [sys.executable, "broadcaster.py"],
        cwd=os.path.dirname(os.path.abspath(__file__)) or ".",
    )

    broadcaster.wait()
    print(f"\nBroadcaster finished at {time.strftime('%H:%M:%S')}")

    print(f"Draining for {DRAIN_TIME}s...")
    time.sleep(DRAIN_TIME)

    print("Stopping all processes...")
    print("Cycle complete.\n")


def main():
    print("Orchestrator started. Ctrl+C to stop.")
    try:
        while True:
            run_cycle()
            print(f"Pausing {PAUSE_BETWEEN}s before next cycle...")
            time.sleep(PAUSE_BETWEEN)
    except KeyboardInterrupt:
        print("\nOrchestrator stopped.")


if __name__ == "__main__":
    main()