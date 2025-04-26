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

    def _check_for_breakpoints(self):
        """Check for breakpoints in code that would crash Ray jobs"""
        import inspect
        import os
        import re

        log.info("Checking for breakpoints in code...")

        # Get the module of the task function
        if self.task_function is None:
            log.warning("No task function available to check for breakpoints")
            return

        module = inspect.getmodule(self.task_function)
        if module is None:
            log.warning("Could not determine module of task function")
            return

        # Get the file path of the module
        try:
            file_path = inspect.getfile(module)
        except TypeError:
            log.warning("Could not determine file path of module")
            return

        # Define breakpoint patterns to search for
        breakpoint_patterns = [
            r"breakpoint\(\)",
            r"pdb\.set_trace\(\)",
            r"import pdb",
            r"import ipdb",
            r"ipdb\.set_trace\(\)",
        ]

        # Recursively check files starting from the entry point
        checked_files = set()
        files_to_check = [file_path]

        breakpoints_found = False

        while files_to_check:
            current_file = files_to_check.pop()

            if current_file in checked_files or not os.path.exists(current_file):
                continue

            checked_files.add(current_file)

            try:
                with open(current_file, "r") as f:
                    content = f.read()

                # Check for breakpoint patterns
                line_number = 1
                for line in content.split("\n"):
                    for pattern in breakpoint_patterns:
                        if re.search(pattern, line):
                            log.warning(
                                f"Potential breakpoint found in {current_file}:{line_number}: {line.strip()}"
                            )
                            breakpoints_found = True
                    line_number += 1

                # TODO: Optionally add logic to find imported modules and add them to files_to_check

            except Exception as e:
                log.warning(f"Error checking file {current_file}: {str(e)}")

        if breakpoints_found:
            log.warning(
                "⚠️ Breakpoints detected! Ray jobs may crash. Please remove breakpoints before submitting jobs."
            )
        else:
            log.info("No breakpoints detected.")

    def launch(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> Sequence[JobReturn]:
        from . import _core

        self._check_for_breakpoints()

        return _core.launch(
            launcher=self, job_overrides=job_overrides, initial_job_idx=initial_job_idx
        )
