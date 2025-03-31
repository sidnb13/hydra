# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Sequence

from omegaconf import DictConfig, OmegaConf
import ray

from hydra.core.hydra_config import HydraConfig
from hydra.core.singleton import Singleton
from hydra.core.utils import (
    JobReturn,
    JobStatus,
    configure_log,
    filter_overrides,
    run_job,
    setup_globals,
)
from hydra.types import HydraContext, TaskFunction

from .container_discovery import (
    discover_project_containers,
    get_current_container_id,
    launch_blocker_job,
)
from .ray_jobs_launcher import RayJobsLauncher

log = logging.getLogger(__name__)


def execute_job(
    hydra_context: HydraContext,
    sweep_config: DictConfig,
    task_function: TaskFunction,
    singleton_state: Dict[Any, Any],
) -> JobReturn:
    setup_globals()
    Singleton.set_state(singleton_state)

    HydraConfig.instance().set_config(sweep_config)

    ret = run_job(
        hydra_context=hydra_context,
        task_function=task_function,
        config=sweep_config,
        job_dir_key="hydra.sweep.dir",
        job_subdir_key="hydra.sweep.subdir",
    )

    return ret


def launch(
    launcher: RayJobsLauncher,
    job_overrides: Sequence[Sequence[str]],
    initial_job_idx: int,
) -> Sequence[JobReturn]:
    """
    :param job_overrides: a List of List<String>, where each inner list is the arguments for one job run.
    :param initial_job_idx: Initial job idx in batch.
    :return: an array of return values from run_job with indexes corresponding to the input list indexes.
    """
    setup_globals()
    assert launcher.config is not None
    assert launcher.task_function is not None
    assert launcher.hydra_context is not None

    # Default to sync mode for test compatibility
    sync_mode = launcher.config.hydra.launcher.poll_jobs

    configure_log(launcher.config.hydra.hydra_logging, launcher.config.hydra.verbose)
    sweep_dir = Path(str(launcher.config.hydra.sweep.dir))
    sweep_dir.mkdir(parents=True, exist_ok=True)

    # Dedicated shared directory for lock files
    lockfile_dir = Path("/root/ray_lockfiles")
    lockfile_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Ray Job Launcher is enqueuing {len(job_overrides)} job(s) in queue")
    log.debug(f"Sweep output dir : {sweep_dir}")
    log.debug(f"Lockfile dir for GPU coordination: {lockfile_dir}")

    if not sweep_dir.is_absolute():
        log.warning(
            "Using relative sweep dir: Please be aware that dir will be relative to where workers are started from."
        )
    if sync_mode:
        log.debug(
            "Running jobs in synchronous mode. Entrypoint will block until all jobs complete."
        )

    pending_jobs, completed_runs = [], []

    # Check if GPU blocking is enabled in the config
    enable_blocking = launcher.config.hydra.launcher.get("enable_gpu_blocking", False)
    # Discover other containers if blocking is enabled
    other_containers = []
    if enable_blocking:
        log.debug("GPU blocking mode enabled")
        current_container_id = get_current_container_id()
        other_containers = discover_project_containers(
            exclude_ray=True, current_container_id=current_container_id
        )
        log.info(
            f"Found {len(other_containers)} other containers for blocking (excluding current)"
        )
    else:
        log.debug("GPU blocking mode disabled")

    for idx, overrides in enumerate(job_overrides, start=initial_job_idx):
        sweep_config = launcher.hydra_context.config_loader.load_sweep_config(
            launcher.config, list(overrides)
        )

        entrypoint_file = os.path.join(
            sweep_config.hydra.runtime.cwd, launcher.original_invocation_path
        )
        python_path = sweep_config.hydra.launcher.python_path or sys.executable
        entrypoint = f"{python_path} {entrypoint_file}"

        override_args = " ".join(
            [f"'{override}'" for override in filter_overrides(overrides)]
        )
        if override_args:
            entrypoint = f"{entrypoint} {override_args}"

        entrypoint_resources = OmegaConf.to_container(
            sweep_config.hydra.launcher.entrypoint_resources
        )
        # Check for nonexistent resources in the cluster
        cluster_resources = ray.cluster_resources()
        if entrypoint_resources:
            for resource_key in entrypoint_resources:
                if resource_key not in cluster_resources:
                    raise ValueError(
                        f"Resource '{resource_key}' requested but not available in Ray cluster. "
                        f"Available resources: {cluster_resources}"
                    )

        job_id = launcher.client.submit_job(
            entrypoint=entrypoint,
            runtime_env=sweep_config.hydra.launcher.runtime_env,
            entrypoint_num_gpus=sweep_config.hydra.launcher.entrypoint_num_gpus,
            entrypoint_num_cpus=sweep_config.hydra.launcher.entrypoint_num_cpus,
            entrypoint_resources=entrypoint_resources,
            metadata={"description": " ".join(filter_overrides(overrides))},
        )

        # Create a lock file for this job if blocking is enabled
        lock_file = None
        active_blockers = []
        if enable_blocking and other_containers:
            # Determine GPU count to block - default to config value or 1
            gpu_count = sweep_config.hydra.launcher.get("entrypoint_num_gpus", 1)

            # Create lock file for coordination
            lock_id = str(uuid.uuid4())
            lock_file = str(lockfile_dir / f"gpu_lock_{lock_id}.lock")
            with open(lock_file, "w") as f:
                f.write(f"LOCK:{str(idx)}\n")

            log.debug(f"Created GPU lock file: {lock_file} for {gpu_count} GPUs")

            # Launch blockers on other containers before submitting the main job
            for container in other_containers:
                blocker_result = launch_blocker_job(
                    container_id=container["id"],
                    container_name=container[
                        "name"
                    ],  # Use the container name for Ray resources
                    lock_file=lock_file,
                    job_id=str(idx),
                    main_job_id=job_id,
                    gpu_count=gpu_count,
                    polling_interval=sweep_config.hydra.launcher.get(
                        "poll_interval", 1.0
                    ),
                )

                if blocker_result["success"]:
                    log.info(
                        f"Successfully launched blocker on container {container['name']}"
                    )
                    active_blockers.append(
                        {
                            "container": container,
                            "blocker_info": blocker_result,
                            "gpu_count": gpu_count,
                        }
                    )
                else:
                    raise RuntimeError(
                        f"Failed to launch blocker on container {container['name']}: {blocker_result['error']}"
                    )

        pending_jobs.append(
            {
                "job_id": job_id,
                "sweep_config": sweep_config,
                "overrides": overrides,
                "idx": idx,
                "lock_file": lock_file,
                "active_blockers": active_blockers,
            }
        )

        log.info(f"Submitted job: {job_id} under python executable: {python_path}")
        log.info(
            f"\t#{idx + 1} : {sweep_config.hydra.job.name} : {' '.join(filter_overrides(overrides))}"
        )

    if sync_mode:
        # Monitor jobs until all complete
        job_seen_logs = {}  # Track logs we've already seen for each job
        while pending_jobs:
            # Check each pending job
            for job in pending_jobs[:]:  # Create copy to allow removal during iteration
                job_info = launcher.client.get_job_info(job["job_id"])
                job_id = job["job_id"]

                # Trail logs for running jobs
                if job_info.status in ["PENDING", "RUNNING"]:
                    try:
                        current_logs = launcher.client.get_job_logs(job_id)
                        # Only show new logs
                        if job_id not in job_seen_logs:
                            job_seen_logs[job_id] = set()

                        # Split logs into lines and show only new ones
                        new_logs = []
                        for line in current_logs.splitlines():
                            line = line.strip()
                            if line and line not in job_seen_logs[job_id]:
                                new_logs.append(line)
                                job_seen_logs[job_id].add(line)

                        if new_logs:
                            log.info(f"Job {job_id} logs:\n" + "\n".join(new_logs))
                    except Exception as e:
                        log.warning(f"Failed to get logs for job {job_id}: {str(e)}")

                if job_info.status in ["FAILED", "STOPPED", "SUCCEEDED"]:
                    ret = JobReturn()
                    ret.working_dir = str(sweep_dir / str(job_id))
                    ret.overrides = list(job["overrides"])

                    # Print final logs if job completed
                    try:
                        final_logs = launcher.client.get_job_logs(job_id)
                        new_logs = []
                        if job_id not in job_seen_logs:
                            job_seen_logs[job_id] = set()

                        for line in final_logs.splitlines():
                            line = line.strip()
                            if line and line not in job_seen_logs[job_id]:
                                new_logs.append(line)
                                job_seen_logs[job_id].add(line)

                        if new_logs:
                            log.info(
                                f"Final logs for job {job_id}:\n" + "\n".join(new_logs)
                            )
                    except Exception as e:
                        log.warning(
                            f"Failed to get final logs for job {job_id}: {str(e)}"
                        )

                    if job_info.status == "SUCCEEDED":
                        ret.status = JobStatus.COMPLETED
                    else:
                        ret.status = JobStatus.FAILED
                        ret.return_value = RuntimeError(
                            f"Ray job failed with status {job_info.status}."
                        )

                    completed_runs.append(ret)
                    pending_jobs.remove(job)
                    job_seen_logs.pop(job_id, None)  # Clean up tracking
                    log.info(f"Job {job_id} completed with status: {job_info.status}")

            if pending_jobs:
                time.sleep(launcher.config.hydra.launcher.poll_jobs)
    else:
        # Original async behavior
        for job in pending_jobs:
            ret = JobReturn()
            ret.working_dir = str(sweep_dir)
            ret.overrides = list(job["overrides"])
            ret.status = JobStatus.COMPLETED
            completed_runs.append(ret)

    # Sort by original index to maintain order
    completed_runs.sort(key=lambda x: job_overrides.index(x.overrides))
    return completed_runs
