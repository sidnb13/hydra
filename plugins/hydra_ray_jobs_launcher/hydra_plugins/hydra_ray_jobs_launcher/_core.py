# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Sequence

from omegaconf import DictConfig

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

    log.info(f"Ray Job Launcher is enqueuing {len(job_overrides)} job(s) in queue")
    log.info(f"Sweep output dir : {sweep_dir}")
    if not sweep_dir.is_absolute():
        log.warning(
            "Using relative sweep dir: Please be aware that dir will be relative to where workers are started from."
        )
    if sync_mode:
        log.info(
            "Running jobs in synchronous mode. Entrypoint will block until all jobs complete."
        )

    pending_jobs, completed_runs = [], []

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

        job_id = launcher.client.submit_job(
            entrypoint=entrypoint,
            runtime_env=sweep_config.hydra.launcher.runtime_env,
            entrypoint_num_gpus=sweep_config.hydra.launcher.entrypoint_num_gpus,
            entrypoint_num_cpus=sweep_config.hydra.launcher.entrypoint_num_cpus,
            metadata={
                "description": " ".join(filter_overrides(overrides)),
            },
        )

        pending_jobs.append(
            {
                "job_id": job_id,
                "sweep_config": sweep_config,
                "overrides": overrides,
                "idx": idx,
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
