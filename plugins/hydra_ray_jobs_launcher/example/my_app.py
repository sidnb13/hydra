# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
import time

from omegaconf import DictConfig

import hydra

log = logging.getLogger(__name__)


@hydra.main(config_name="config", config_path="config", version_base=None)
def my_app(cfg: DictConfig) -> None:
    log.info(f"Process ID {os.getpid()} executing task {cfg.task} ...")

    time.sleep(1)

    if cfg.bruh == 3:
        exit(1)


if __name__ == "__main__":
    my_app()
