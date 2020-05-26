import os
import glob
import tempfile
import subprocess
import traceback
from setuptools import setup, find_packages
from setuptools.command import install, build_py, sdist


install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]


def _run(cmd: list, **kwargs):
    p = subprocess.run(cmd, **kwargs)
    p.check_returncode()
    return p


class BuildPy(build_py.build_py):
    def run(self):
        super().run()
        if not self.dry_run:
            try:
                _run(["tar", "xjf", "htslib.tar.bz2", "-C", "build"])
                _run(["tar", "xjf", "bcftools.tar.bz2", "-C", "build"])
                _run(["make"], cwd="build/htslib")
                _run(["make"], cwd="build/bcftools")
            except subprocess.CalledProcessError:
                print("Failed to build htslib/bcftools:")
                traceback.print_exc()


class Install(install.install):
    def run(self):
        super().run()
        if not self.dry_run:
            try:
                root = os.path.dirname(os.path.abspath(__file__))
                bindir = os.path.join(root, os.path.abspath(self.install_scripts))
                libdir = os.path.join(root, os.path.abspath(self.install_lib))
                includedir = os.path.join(root, os.path.abspath(self.install_headers))
                datadir = os.path.join(root, os.path.abspath(self.install_data))
                _run(["make",
                      f"bindir={bindir}",
                      f"includedir={includedir}",
                      f"libdir={libdir}",
                      f"libexecdir={libdir}",
                      f"datarootdir={datadir}",
                      "INSTALL_MAN=:",
                      "install"], cwd="build/htslib")
                _run(["make",
                      f"bindir={bindir}",
                      f"libdir={libdir}",
                      f"libexecdir={libdir}",
                      "INSTALL_MAN=:",
                      "install"], cwd="build/bcftools")
            except subprocess.CalledProcessError:
                print("Failed to package htslib/bcftools")
                traceback.print_exc()


setup(
    name='xsamtools',
    version='0.1.0',
    description='Lightly modified versions of htslib and bcftools.',
    url='https://github.com/xbrianh/xsamtools.git',
    author='Brian Hannafious',
    author_email='bhannafi@ucsc.edu',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    scripts=glob.glob('scripts/*'),
    zip_safe=False,
    install_requires=install_requires,
    platforms=['MacOS X', 'Posix'],
    test_suite='test',
    cmdclass=dict(install=Install, build_py=BuildPy),
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ]
)
