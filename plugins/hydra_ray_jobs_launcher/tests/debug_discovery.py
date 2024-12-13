from re import A
from hydra.core.plugins import Plugins
from hydra.plugins.launcher import Launcher
from hydra.plugins.plugin import Plugin
from hydra.test_utils.test_utils import chdir_plugin_root
import sys
import inspect
import logging

# Enable debug logging for Hydra
logging.basicConfig(level=logging.DEBUG)

# Print Python path
print("Python path:")
for p in sys.path:
    print(f"  - {p}")

chdir_plugin_root()

# Try to import directly and inspect
try:
    from hydra_plugins.hydra_ray_jobs_launcher.ray_jobs_launcher import RayJobsLauncher
    print("\nLauncher inspection:")
    print(f"  Module: {RayJobsLauncher.__module__}")
    print(f"  File: {inspect.getfile(RayJobsLauncher)}")
    print(f"  Is Launcher subclass: {issubclass(RayJobsLauncher, Launcher)}")
    print(f"  MRO: {RayJobsLauncher.__mro__}")
except ImportError as e:
    print(f"\nFailed to import: {e}")

# Force new plugin instance
plugins = Plugins()

# Print discovered plugins with full details
print("\nDiscovered launchers:")
discovered = plugins.discover(Launcher)
for plugin in discovered:
    print(f"\n  Plugin: {plugin.__name__}")
    print(f"    Module: {plugin.__module__}")
    print(f"    MRO: {plugin.__mro__}")

# Also try discovering all plugin types
print("\nAll discovered plugins:")
for plugin in plugins.discover(Plugin):
    print(f"  {plugin.__name__} from {plugin.__module__}")

# Add to debug_discovery.py after line 31
import importlib.util
print("\nChecking for plugin package:")
spec = importlib.util.find_spec("hydra_plugins.hydra_ray_jobs_launcher")
print(f"Found spec: {spec}")
if spec:
    print(f"Origin: {spec.origin}")


# Add after the spec check
print("\nNamespace contents:")
import hydra_plugins
print(f"hydra_plugins.__path__: {hydra_plugins.__path__}")
import pkgutil
print("\nPackages in hydra_plugins:")
for finder, name, ispkg in pkgutil.iter_modules(hydra_plugins.__path__):
    print(f"  {name} (ispkg={ispkg}) from {finder}")