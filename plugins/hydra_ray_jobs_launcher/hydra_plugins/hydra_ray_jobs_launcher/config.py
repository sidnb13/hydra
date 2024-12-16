# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from dataclasses import dataclass
from typing import Optional, Union

from hydra.core.config_store import ConfigStore


@dataclass
class RayJobsLauncherConf:
    _target_: str = (
        "hydra_plugins.hydra_ray_jobs_launcher.ray_jobs_launcher.RayJobsLauncher"
    )
    poll_jobs: bool = True
    poll_interval: float = 0.5
    entrypoint: Optional[str] = None
    address: str = "http://127.0.0.1:8265"  # Ray Dashboard address
    working_dir: Optional[str] = None
    runtime_env: Optional[dict] = None
    job_id: Optional[str] = None
    metadata: Optional[dict] = None
    entrypoint_num_cpus: Optional[Union[int, float]] = None
    entrypoint_num_gpus: Optional[Union[int, float]] = None
    entrypoint_memory: Optional[int] = None


ConfigStore.instance().store(
    group="hydra/launcher",
    name="ray_jobs",
    node=RayJobsLauncherConf,
    provider="ray_jobs_launcher",
)
