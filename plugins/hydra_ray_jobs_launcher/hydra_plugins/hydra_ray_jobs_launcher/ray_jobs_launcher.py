# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
from typing import Any, Optional, Sequence

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

        self.client = JobSubmissionClient(
            self.client_conf.address, create_cluster_if_needed=True
        )

    def launch(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> Sequence[JobReturn]:
        from . import _core

        return _core.launch(
            launcher=self, job_overrides=job_overrides, initial_job_idx=initial_job_idx
        )
