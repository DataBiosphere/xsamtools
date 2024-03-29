import os
import glob
import subprocess
import traceback
from setuptools import setup, find_packages
from setuptools.command import install, build_py


SAMTOOLS_VERSION = '1.15.1'
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
                for tool in ['samtools', 'htslib', 'bcftools']:
                    if not os.path.exists(f'build/{tool}'):
                        if not os.path.exists(f'{tool}.tar.bz2'):
                            _run(['wget',
                                  f'https://github.com/samtools/{tool}/releases/download/{SAMTOOLS_VERSION}/{tool}-{SAMTOOLS_VERSION}.tar.bz2',
                                  '-O', f'{tool}.tar.bz2'])
                        os.makedirs(f'build/{tool}', exist_ok=True)
                        _run(["tar", "-vxjf", f"{tool}.tar.bz2", f"--directory=build/{tool}", "--strip-components=1"])

                for tool in ['samtools', 'htslib', 'bcftools']:
                    _run(["./configure"], cwd=f"build/{tool}")
                    _run(["make"], cwd=f"build/{tool}")
            except subprocess.CalledProcessError:
                print("Failed to build samtools/htslib/bcftools:")
                traceback.print_exc()
                raise

class Install(install.install):
    def run(self):
        super().run()
        if not self.dry_run:
            root = os.path.dirname(os.path.abspath(__file__))
            bindir = os.path.join(root, os.path.abspath(self.install_scripts))
            datadir = os.path.join(root, os.path.abspath(self.install_data))
            libdir = os.path.join(root, os.path.abspath(self.install_lib))
            includedir = os.path.join(root, os.path.abspath(self.install_headers))
            try:
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
                raise

with open("README.md") as fh:
    long_description = fh.read()

def get_version():
    filepath = os.path.join(os.path.dirname(__file__), "xsamtools", "version.py")
    with open(filepath) as fh:
        version = dict()
        exec(fh.read().strip(), version)
        return version['__version__']

setup(
    name='xsamtools',
    version=get_version(),
    description='Lightly modified versions of samtools, htslib, and bcftools.',
    long_description=long_description,
    long_description_content_type='text/markdown',
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
    cmdclass=dict(install=Install,
                  build_py=BuildPy),
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ]
)
