# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import sys
from typing import Any, Optional, Sequence

import ray
from omegaconf import DictConfig, OmegaConf
from ray.job_submission import JobSubmissionClient

from hydra.core.utils import JobReturn
from hydra.plugins.launcher import Launcher
from hydra.types import HydraContext, TaskFunction

from .config import RayJobsLauncherConf

log = logging.getLogger(__name__)


class RayJobsLauncher(Launcher):
    def __init__(self, **params: Any) -> None:
        """Ray Jobs Launcher

        Launches jobs using Ray Jobs. For details, refer to:
        https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html
        """
        self.config: Optional[DictConfig] = None
        self.task_function: Optional[TaskFunction] = None
        self.hydra_context: Optional[HydraContext] = None

        self.client_conf = OmegaConf.structured(RayJobsLauncherConf(**params))

    def setup(
        self,
        *,
        hydra_context: HydraContext,
        task_function: TaskFunction,
        config: DictConfig,
    ) -> None:
        self.config = config
        self.task_function = task_function
        self.hydra_context = hydra_context
        self.original_invocation_path = sys.argv[0]

        ray_address = self.config.hydra.launcher.get("ray_address", None)

        if ray_address:
            # For dashboard URLs, extract the address information
            if ray_address.startswith("http://"):
                # Convert http://127.0.0.1:8275 to 127.0.0.1:6379
                # Ray typically uses port 6379 for client connections
                host = ray_address.split("://")[1].split(":")[0]
                ray_client_address = f"{host}:6379"  # Default Ray client port
                log.info(f"Connecting to existing Ray cluster at: {ray_client_address}")
                ray.init(address=ray_client_address, ignore_reinit_error=True)
                # Still use the dashboard URL for the JobSubmissionClient
                self.client = JobSubmissionClient(ray_address)
            else:
                # For direct ray:// addresses
                log.info(f"Connecting to existing Ray cluster at: {ray_address}")
                ray.init(address=ray_address, ignore_reinit_error=True)
                self.client = JobSubmissionClient(ray_address)
        else:
            log.info("Initializing Ray locally")
            ray.init(ignore_reinit_error=True)
            self.client = JobSubmissionClient("auto")

    def launch(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> Sequence[JobReturn]:
        from . import _core

        return _core.launch(
            launcher=self, job_overrides=job_overrides, initial_job_idx=initial_job_idx
        )
