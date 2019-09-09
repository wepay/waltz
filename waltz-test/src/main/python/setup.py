from setuptools import find_packages, setup

versions = {
      "ducktape": "0.7.1"
}

setup(name="waltz_ducktape",
      version="0.0.1",
      description="Waltz Integration Tests",
      platforms=["any"], 
      packages=find_packages(),
      include_package_data=True,
      install_requires=["%s==%s" % (k, v) for k, v in versions.items()],
      tests_require=["pytest", "mock"],
      )
