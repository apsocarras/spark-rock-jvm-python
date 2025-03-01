import os
import zipfile
from typing import override

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py

PACKAGE_NAME = "spark-rock-jvm-python"
PACKAGE_DIR_NAME = PACKAGE_NAME.replace("-", "_")


class my_build_py(build_py):
    """Unzip data directory during package build step"""

    @override
    def run(self) -> None:
        src_dir = os.path.abspath(os.path.join("src", PACKAGE_DIR_NAME))
        zip_path = os.path.join(src_dir, "data.zip")

        if not os.path.exists(zip_path):
            error_msg = f"Missing '{zip_path}. Ensure package was built correctly and includes zip file."
            raise FileNotFoundError(error_msg)

        extract_to = os.path.abspath(os.path.join(src_dir, "resources"))
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)

        super().run()


_ = setup(
    name=PACKAGE_NAME,
    use_scm_version=True,
    package_dir={"": "src"},
    packages=find_packages(where="src", exclude={"test*", "testing*", "tests*"}),
    cmdclass={"build_py": my_build_py},
    include_package_data=True,
    package_data={PACKAGE_DIR_NAME: ["*.zip", ".gz"]},
)
