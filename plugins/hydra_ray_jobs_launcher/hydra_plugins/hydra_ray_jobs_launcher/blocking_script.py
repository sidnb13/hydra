"""Standalone blocker script for use with ray job submit"""

import argparse
import logging
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[BLOCKER %(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ray_blocker")


def main():
    parser = argparse.ArgumentParser(description="Ray GPU resource blocker")
    parser.add_argument(
        "--lock-file", type=str, required=True, help="Path to lock file"
    )
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds",
    )
    parser.add_argument(
        "--gpu-count", type=int, default=1, help="Number of GPUs being blocked"
    )
    parser.add_argument("--job-id", type=str, help="ID of the job we're blocking for")
    args = parser.parse_args()

    lock_path = Path(args.lock_file)

    log.info(f"Starting GPU blocker for {args.gpu_count} GPUs (job: {args.job_id})")
    log.info(f"Monitoring lock file: {lock_path}")
    log.info(f"Polling interval: {args.polling_interval}s")

    # Keep running until lock file is removed
    try:
        while lock_path.exists():
            log.debug(f"Lock file exists, continuing to block {args.gpu_count} GPUs")
            time.sleep(args.polling_interval)

        log.info(f"Lock file removed, releasing {args.gpu_count} GPU resources")
    except KeyboardInterrupt:
        log.info("Blocker interrupted by user")
    except Exception as e:
        log.error(f"Error in blocker: {str(e)}")
        return 1

    log.info("Blocker exiting normally")
    return 0


if __name__ == "__main__":
    sys.exit(main())
