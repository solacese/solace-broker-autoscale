"""Build hooks that keep wheels independent of ignored workspace artifacts."""

from pathlib import Path
from shutil import rmtree

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


class CleanBuildPy(_build_py):
    """Remove prior package copies before setuptools populates ``build/lib``."""

    def run(self) -> None:
        project = Path(__file__).resolve().parent
        build_root = Path(self.build_lib).resolve()
        generated = (project / "build").resolve()
        if build_root != generated and generated not in build_root.parents:
            raise RuntimeError("refusing to clean a build directory outside project/build")
        for package in ("solace_autoscale", "solace_autoscale_client"):
            rmtree(build_root / package, ignore_errors=True)
        super().run()


setup(cmdclass={"build_py": CleanBuildPy})
