"""Standalone blocker script for use with ray job submit"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from ray.job_submission import JobSubmissionClient

logging.basicConfig(
    level=logging.INFO,
    format="[BLOCKER %(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ray_blocker")


def main():
    start_time = time.time()  # Add start time tracking

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
    parser.add_argument(
        "--main-job-id",
        type=str,
        help="Ray job ID of the main job we're blocking for",
    )
    args = parser.parse_args()

    # Print diagnostic info at startup
    log.info(f"Script location: {__file__}")
    log.info(f"Current directory: {os.getcwd()}")
    log.info(f"Python executable: {sys.executable}")
    log.info(f"Ray lockfiles directory exists: {os.path.exists('/root/ray_lockfiles')}")

    lock_path = Path(args.lock_file)
    log.info(f"Lock file path: {lock_path}")
    log.info(f"Lock file exists: {lock_path.exists()}")
    log.info(f"Main Ray job ID: {args.main_job_id}")

    log.info(f"Starting GPU blocker for {args.gpu_count} GPUs (job: {args.job_id})")
    log.info(f"Monitoring lock file: {lock_path}")
    log.info(f"Polling interval: {args.polling_interval}s")

    # Initialize Ray Job Client
    client = JobSubmissionClient("auto")

    # Keep running until lock file is removed or main job completes
    try:
        while True:
            if not lock_path.exists():
                log.info(f"Lock file removed, releasing {args.gpu_count} GPU resources")
                break

            if args.main_job_id:
                try:
                    job_info = client.get_job_info(args.main_job_id)
                    status = job_info.status

                    log.debug(f"Main job status: {status}")

                    if status in ["FAILED", "STOPPED", "SUCCEEDED"]:
                        log.info(f"Main job exited with status: {status}")
                        # Remove lock file ourselves to clean up
                        try:
                            if lock_path.exists():
                                os.remove(lock_path)
                                log.info(f"Removed lock file: {lock_path}")
                        except Exception as e:
                            log.warning(f"Failed to remove lock file: {str(e)}")
                        break
                except Exception as e:
                    log.warning(f"Failed to check job status: {str(e)}")
                    # If we can't check the job status, fall back to lock file check

            # Sleep before checking again
            time.sleep(args.polling_interval)

    except KeyboardInterrupt:
        log.info("Blocker interrupted by user")
    except Exception as e:
        log.error(f"Error in blocker: {str(e)}")
        return 1
    finally:
        # Try to remove lock file on exit if it exists
        try:
            if lock_path.exists():
                os.remove(lock_path)
                log.info(f"Removed lock file during cleanup: {lock_path}")
        except Exception as e:
            log.warning(f"Failed to remove lock file during cleanup: {str(e)}")

        # Log the total execution time
        elapsed_time = time.time() - start_time
        log.info(f"Total execution time: {elapsed_time:.2f} seconds")

    log.info("Blocker exiting normally")
    return 0


if __name__ == "__main__":
    sys.exit(main())
