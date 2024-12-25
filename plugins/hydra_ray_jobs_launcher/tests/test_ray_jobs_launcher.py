# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from pathlib import Path
from typing import List

import pytest
from hydra_plugins.hydra_ray_jobs_launcher.ray_jobs_launcher import RayJobsLauncher
from hydra.core.plugins import Plugins
from hydra.plugins.launcher import Launcher
from hydra.core.utils import JobStatus
from hydra.test_utils.launcher_common_tests import (
    IntegrationTestSuite,
    LauncherTestSuite,
)
from hydra.test_utils.test_utils import TSweepRunner, chdir_plugin_root

chdir_plugin_root()


def test_discovery() -> None:
    # Tests that this plugin can be discovered via the plugins subsystem
    assert RayJobsLauncher.__name__ in [
        x.__name__ for x in Plugins.instance().discover(Launcher)
    ]


@pytest.mark.parametrize(
    "launcher_name, overrides",
    [
        (
            "ray_jobs",
            [],
        )
    ],
)
class TestRayJobsLauncher(LauncherTestSuite):
    """Run the Launcher test suite on Ray Jobs launcher"""

    pass


@pytest.mark.parametrize(
    "task_launcher_cfg, extra_flags",
    [
        (
            {},
            [
                "-m",
                "hydra/launcher=ray_jobs",
            ],
        )
    ],
)
class TestRayJobsLauncherIntegration(IntegrationTestSuite):
    """Run the integration test suite with Ray Jobs launcher"""

    pass


@pytest.mark.parametrize(
    "params_overrides",
    [
        [],  # Test with defaults
        ["hydra.launcher.entrypoint_num_cpus=2"],  # Test with custom CPU allocation
        ["hydra.launcher.entrypoint_num_gpus=1"],  # Test with GPU request
    ],
)
def test_example_app(
    hydra_sweep_runner: TSweepRunner, params_overrides: List[str]
) -> None:
    """Test the launcher with various configuration overrides"""
    with hydra_sweep_runner(
        calling_file=str(Path(__file__).parent / "example/my_app.py"),
        calling_module=None,
        task_function=None,
        config_path=".",
        config_name="config",
        overrides=["task=1,2,3"] + params_overrides,
    ) as sweep:
        # Verify sweep returns exist and have correct number of jobs
        assert sweep.returns is not None
        assert len(sweep.returns[0]) == 3

        # Verify all jobs were submitted successfully (not waiting for completion)
        for ret in sweep.returns[0]:
            assert (
                ret.status == JobStatus.COMPLETED
            )  # Jobs are marked completed upon submission
            assert ret.working_dir is not None  # Working directory should be set
