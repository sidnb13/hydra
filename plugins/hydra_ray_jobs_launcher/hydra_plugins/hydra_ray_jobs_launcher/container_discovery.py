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
        if current_container_id and container.id.startswith(current_container_id):
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
    lock_file: str,
    job_id: str,
    gpu_count: int,
    polling_interval: float = 1.0,
) -> Dict[str, Any]:
    """Launch a blocker job on another container using ray job submit CLI"""
    try:
        # Import the blocking_script module and get its source code
        from . import blocking_script

        script_source = inspect.getsource(blocking_script)

        # Create a temporary script file on the host
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp:
            temp.write(script_source)
            temp_script_path = temp.name

        # Docker cp command to copy the script to the container
        container_script_path = f"/tmp/blocker_{job_id}.py"
        cp_cmd = [
            "docker",
            "cp",
            temp_script_path,
            f"{container_id}:{container_script_path}",
        ]

        log.debug(f"Copying blocker script to container {container_id}")
        cp_result = subprocess.run(
            cp_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Clean up the temporary file
        try:
            os.unlink(temp_script_path)
        except:
            pass

        if cp_result.returncode != 0:
            log.error(
                f"Failed to copy blocker script to container {container_id}: {cp_result.stderr}"
            )
            return {
                "container_id": container_id,
                "success": False,
                "error": cp_result.stderr,
            }

        # Build ray job submit command
        cmd = [
            "ray",
            "job",
            "submit",
            "--entrypoint-num-gpus",
            str(gpu_count),
            "--entrypoint-resources",
            f'{{"container_id": "{container_id}"}}',
            "--",
            "python",
            container_script_path,
            "--lock-file",
            lock_file,
            "--polling-interval",
            str(polling_interval),
            "--gpu-count",
            str(gpu_count),
            "--job-id",
            job_id,
        ]

        log.debug(f"Launching blocker on container {container_id}: {' '.join(cmd)}")

        # Execute the command
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        if result.returncode != 0:
            log.error(
                f"Failed to launch blocker on container {container_id}: {result.stderr}"
            )
            return {
                "container_id": container_id,
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
            "blocker_job_id": blocker_job_id,
            "script_path": container_script_path,
            "success": True,
        }
    except Exception as e:
        log.error(f"Error launching blocker on container {container_id}: {str(e)}")
        return {"container_id": container_id, "success": False, "error": str(e)}
