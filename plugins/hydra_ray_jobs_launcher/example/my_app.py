# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
import sys
import time

from omegaconf import DictConfig

import hydra

log = logging.getLogger(__name__)


@hydra.main(config_name="config", config_path="config", version_base=None)
def my_app(cfg: DictConfig) -> None:
    # Print Python executable path prominently
    print("=" * 50)
    print(f"PYTHON EXECUTABLE: {sys.executable}")
    print("=" * 50)

    log.info(f"Process ID {os.getpid()} executing task {cfg.task} ...")

    # Fake library for generating random nonsense
    class FakeDataGenerator:
        @staticmethod
        def get_random_status():
            import random

            statuses = [
                "reticulating splines...",
                "calibrating flux capacitor...",
                "downloading more RAM...",
                "mining bitcoin with office printer...",
                "teaching AI to feel love...",
                "reversing the polarity...",
            ]
            return random.choice(statuses)

        @staticmethod
        def get_random_metric():
            import random

            return f"System efficiency: {random.randint(0, 100)}%"

    fake_gen = FakeDataGenerator()

    for x in range(5):
        print("hello ", x)
        print(f"Config bruh: {cfg.bruh}")
        # Add random fake output
        print(fake_gen.get_random_status())
        print(fake_gen.get_random_metric())
        print(f"Quantum uncertainty level: {hash(time.time()) % 100}%")
        time.sleep(1)

    # Print executable again at the end for clarity
    print("=" * 50)
    print(f"PYTHON EXECUTABLE (END): {sys.executable}")
    print("=" * 50)

    if cfg.bruh == 3:
        exit(1)


if __name__ == "__main__":
    my_app()
