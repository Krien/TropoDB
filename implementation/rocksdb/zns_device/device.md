# DO NOT USE THIS SOURCE!!!
The project is moved to another repository, [SimpleZNSDevice](https://github.com/Krien/SimpleZNSDevice). It is kept here for legacy purposes only.

# Device
Device is an interface that we will use to interface with the ZNS device. It comes with an interface that allows to write to zones without worrying about SPDK internals. There are functions for reading lbas, writing lbas, erasing zones, getting write heads of zones and getting general device information.
It comes with a:
* Shared library called `znsdevicelib`. To be used in our key-value store.
* Standalone executable for CLI, run with `./bin/znsdevice`. To be used for debugging, e.g. resetting zones, reading from the device, getting device information.
* Set of unittests to ensure validity. Automatically tested when CMake is used, except when turned off.

# DISCLAIMER
SPDK needs to bind the device. Therefore ensure that your device is actually binded to SPDK. This can be done by navigating to the `scripts` directory in the SPDK directory and calling `setup.sh`. There are also some helper scripts in the repository root/scripts, bind binds all devices to SPDK and unbind unbinds all devices to SPDK. Further on, always run as root with e.g. `sudo`, the permissions are needed. The code is only tested on GNU/Linux. It will NOT run properly on either Windows. FreeBSD, MacOs etc. are all not tested and will probably not work as well. We make NO guarantees on security or safety, run at own risk for now.

# How to build
There are multiple ways to built this project.
In principal only CMake and SPDK and its dependencies are needed. However, please do set the SPDK dir with an environment variable `SPDK_DIR`, otherwise the dir is not found, since there is no standard SPDK location.
Then it should compile (tested on Ubuntu 20.04 LTS), but no guarantees are made. For example:
```bash
export SPDK_DIR=/home/$USER/local/spdk
rm -f CMakeCache.txt
cmake .
make <TARGET>
```
if the output does not change, try cleaning the build first with:
```bash
make clean
```
## Faster builds
Builds can be slow. Therefore, we use some tricks ourself to speed up compilation, which might aid your development as well. To further speed up compilation, we advise to use alternative build tools such as `Ninja` instead of plain old `Makefiles` and to use faster linkers such as `mold` (other linkers such as GNU Gold and LDD might work as well at an increased speed, but not tested). For example:
```bash
# Remove old build artifacts first if changing build tools.
rm -f CMakeCache.txt
# Make cmake use another build tool with -G.
cmake -j$(nproc) -GNinja .
# cleaning (to force rebuilding).
mold -run ninja build.ninja clean
# Actual build, generally only this command needs to be run incrementally during development.
mold -run ninja build.ninja <TARGET>
```

# Code guidelines
## Where to put code
Please create all headers with `.h` and put them in `./include`. Similarly, please create all implementation code with `.cpp` and put them in `./src`.
## Formatting
If submitting code, please format the code. This prevents spurious commit changes. We use `clang-format` with the config in `.clang-format`, based on `LLVM`. It is possible to automatically format with make or ninja (depending on what build tool is used). This requires clang-format to be either set in `/home/$USER/bin/clang-format` or requires setting the environmental variable `CLANG_FORMAT_PATH` directly (e.g. `export CLANG_FORMAT_PATH=...`). Then simply run:
```bash
<make_command(make,ninja,...)> format
```
## Tests
If submitting code, please ensure that the tests still pass. Either run all tests manually by building and running all tests, such as `<make_command(make,ninja,...)> device_initial_test && sudo ./bin/device_initial_test`. Or use the standard builtin testing functionalities of CMake by running:
```bash
<make_command(make,ninja,...)> test
```
## Documentation
Documentation is generated with Doxygen. This can be done with:
```bash
<make_command(make,ninja,...)> docs
```
