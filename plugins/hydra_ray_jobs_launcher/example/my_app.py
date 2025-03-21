# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
import sys
import time
import random
from omegaconf import DictConfig
import hydra

log = logging.getLogger(__name__)

def cowsay(message):
    message_len = len(message)
    border = "-" * (message_len + 2)
    return f"""
     {border}
    < {message} >
     {border}
            \\   ^__^
             \\  (oo)\\_______
                (__)\\       )/\\
                    ||----w |
                    ||     ||
    """

@hydra.main(config_name="config", config_path="config", version_base=None)
def my_app(cfg: DictConfig) -> None:
    # Print Python executable path prominently
    print("=" * 50)
    print(f"PYTHON EXECUTABLE: {sys.executable}")
    print("=" * 50)

    log.info(f"Process ID {os.getpid()} executing task {cfg.task} ...")

    # Funny status messages
    statuses = [
        "reticulating splines...",
        "calibrating flux capacitor...",
        "downloading more RAM...",
        "mining bitcoin with office printer...",
        "teaching AI to feel love...",
        "reversing the polarity...",
        "hacking the mainframe...",
        "buffering at 99%...",
        "finding your lost socks...",
        "turning it off and on again...",
    ]

    for x in range(5):
        print(f"Iteration {x} with config bruh: {cfg.bruh}")
        
        # Generate random funny status
        status = random.choice(statuses)
        print(cowsay(status))
        
        # Print some random metrics
        print(f"System efficiency: {random.randint(0, 100)}%")
        print(f"Quantum uncertainty level: {hash(time.time()) % 100}%")
        print(f"Ray happiness rating: {random.uniform(0, 10):.2f}/10")
        
        time.sleep(1)

    # Print executable again at the end for clarity
    print("=" * 50)
    print(f"PYTHON EXECUTABLE (END): {sys.executable}")
    print("=" * 50)

    # Exit with error code if bruh == 3
    if cfg.bruh == 3:
        print(cowsay("Exiting with error code 1... bruh moment"))
        exit(1)
    else:
        print(cowsay("Task completed successfully!"))

if __name__ == "__main__":
    my_app()