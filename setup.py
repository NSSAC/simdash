from setuptools import setup

package_name = "simdash"
description = "A web based dashboard for visualizing simulations"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=package_name,
    description=description,

    maintainer="Parantapa Bhattacharya",
    maintainer_email="pb+pypi@parantapa.net",

    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=[package_name],
    scripts=["bin/%s" % package_name],

    use_scm_version=True,
    setup_requires=['setuptools_scm'],

    install_requires=[
        "click",
        "click_completion",
        "logbook",

        "flask",
        "altair",
        "pandas"
    ],

    url="http://github.com/NSSAC/%s" % package_name,
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
