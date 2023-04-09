from setuptools import setup
import os

VERSION = "0.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="sqlite-history",
    description="Track changes to SQLite tables using triggers",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/sqlite-history",
    project_urls={
        "Issues": "https://github.com/simonw/sqlite-history/issues",
        "CI": "https://github.com/simonw/sqlite-history/actions",
        "Changelog": "https://github.com/simonw/sqlite-history/releases",
    },
    license="Apache License, Version 2.0",
    version=VERSION,
    packages=["sqlite_history"],
    install_requires=[],
    extras_require={"test": ["pytest", "sqlite-utils", "cogapp"]},
    python_requires=">=3.7",
)
