# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Sequence

from omegaconf import DictConfig
from ray.job_submission import JobInfo

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

    configure_log(launcher.config.hydra.hydra_logging, launcher.config.hydra.verbose)
    sweep_dir = Path(str(launcher.config.hydra.sweep.dir))
    sweep_dir.mkdir(parents=True, exist_ok=True)

    jobs: List[JobInfo] = []
    log.info(f"Ray Job Launcher is enqueuing {len(job_overrides)} job(s) in queue")
    log.info(f"Sweep output dir : {sweep_dir}")
    if not sweep_dir.is_absolute():
        log.warning(
            "Using relative sweep dir: Please be aware that dir will be relative to where workers are started from."
        )

    async_runs: List[JobStatus] = []

    for idx, overrides in enumerate(job_overrides, start=initial_job_idx):
        sweep_config = launcher.hydra_context.config_loader.load_sweep_config(
            launcher.config, list(overrides)
        )

        # Construct full path to entrypoint python
        entrypoint_file = os.path.join(
            sweep_config.hydra.runtime.config_sources[1].path,
            sweep_config.hydra.job.name,
        )
        
        entrypoint = f"python {entrypoint_file}.py"
        
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
            entrypoint_memory=sweep_config.hydra.launcher.entrypoint_memory,
            metadata={
                "description": " ".join(filter_overrides(overrides)),
            },
        )

        jobs.append(launcher.client.get_job_info(job_id))

        log.info(f"Submitted job: {job_id}")
        log.info(f"\t#{idx+1} : {sweep_config.hydra.job.name} : {' '.join(filter_overrides(overrides))}")

        ret = JobReturn()
        ret.working_dir = str(sweep_dir)
        ret.status = JobStatus.COMPLETED  # Mark as completed since we're not waiting
        async_runs.append(ret)
        async_runs.append(ret)

    return async_runs
