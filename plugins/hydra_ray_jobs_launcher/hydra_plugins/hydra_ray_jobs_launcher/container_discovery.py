# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import inspect
import logging
import os
import subprocess
import tempfile
from typing import Any, Dict

import docker

log = logging.getLogger(__name__)


def get_current_container_id():
    """Get the current container ID from /proc/self/cgroup"""
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                if "docker" in line:
                    return line.split("/")[-1].strip()
    except (FileNotFoundError, IOError):
        log.warning("Could not determine current container ID from cgroups")

    # Fallback to environment variable sometimes set in container environments
    return os.environ.get("HOSTNAME", None)


def discover_project_containers(exclude_ray=True, current_container_id=None):
    """Discover all project containers, excluding the current one and optionally Ray containers"""
    # Get current container ID if not provided
    if not current_container_id:
        current_container_id = get_current_container_id()
        if not current_container_id:
            log.warning("Could not determine current container ID")

    log.info(f"Current container ID: {current_container_id}")

    # Connect to Docker API
    try:
        client = docker.from_env()
    except Exception as e:
        log.error(f"Failed to connect to Docker API: {str(e)}")
        return []

    # Get all running containers
    try:
        containers = client.containers.list()
    except Exception as e:
        log.error(f"Failed to list containers: {str(e)}")
        return []

    # Filter containers
    project_containers = []
    for container in containers:
        # Skip current container
        if current_container_id and container.name == current_container_id:
            continue

        # Skip Ray containers if requested
        if exclude_ray and (
            container.name
            and "ray" in container.name.lower()
            or hasattr(container, "image")
            and container.image
            and hasattr(container.image, "tags")
            and container.image.tags
            and any("ray" in tag.lower() for tag in container.image.tags)
        ):
            continue

        try:
            project_containers.append(
                {
                    "id": container.id,
                    "name": container.name,
                    "hostname": container.attrs["Config"]["Hostname"],
                    "labels": container.labels,
                }
            )
        except Exception as e:
            log.warning(f"Error processing container {container.id}: {str(e)}")

    log.info(f"Discovered {len(project_containers)} other project containers")
    return project_containers


def launch_blocker_job(
    container_id: str,
    container_name: str,
    lock_file: str,
    job_id: str,
    main_job_id: str,
    gpu_count: int,
    polling_interval: float = 1.0,
) -> Dict[str, Any]:
    """Launch a blocker job on another container using ray job submit CLI"""
    try:
        # Create the script in the shared lock directory (only if it doesn't exist)
        shared_dir = "/root/ray_lockfiles"
        os.makedirs(shared_dir, exist_ok=True)
        shared_script_path = os.path.join(shared_dir, "ray_blocker.py")

        # Only create the script if it doesn't exist
        if not os.path.exists(shared_script_path):
            # Import the blocking_script module and get its source code
            from . import blocking_script

            script_source = inspect.getsource(blocking_script)

            # Write the script to the shared location
            log.info(
                f"Creating blocker script at shared location: {shared_script_path}"
            )
            with open(shared_script_path, "w") as f:
                f.write(script_source)

            # Make it executable
            os.chmod(shared_script_path, 0o755)

            log.info(f"Created blocker script: {shared_script_path}")
        else:
            log.info(f"Using existing blocker script: {shared_script_path}")

        # Build ray job submit command for the target container
        cmd = [
            "ray",
            "job",
            "submit",
            "--no-wait",
            "--entrypoint-num-gpus",
            str(gpu_count),
            "--entrypoint-resources",
            f'{{"{container_name}": {gpu_count}}}',
            "--",
            "python",
            shared_script_path,
            "--lock-file",
            lock_file,
            "--polling-interval",
            str(polling_interval),
            "--gpu-count",
            str(gpu_count),
            "--job-id",
            job_id,
            "--main-job-id",
            main_job_id,
        ]

        log.info(f"Launching blocker on container {container_name}: {' '.join(cmd)}")

        # Execute the command
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        if result.returncode != 0:
            log.error(
                f"Failed to launch blocker on container {container_name}: {result.stderr}"
            )
            return {
                "container_id": container_id,
                "container_name": container_name,
                "success": False,
                "error": result.stderr,
            }

        # Extract job ID from output
        blocker_job_id = None
        for line in result.stdout.splitlines():
            if "Job submission successful:" in line:
                blocker_job_id = line.split(":")[-1].strip()
                break

        return {
            "container_id": container_id,
            "container_name": container_name,
            "blocker_job_id": blocker_job_id,
            "script_path": shared_script_path,
            "success": True,
        }
    except Exception as e:
        log.error(f"Error launching blocker on container {container_name}: {str(e)}")
        return {
            "container_id": container_id,
            "container_name": container_name,
            "success": False,
            "error": str(e),
        }
