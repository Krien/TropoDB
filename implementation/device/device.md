# Device
Device is an interface that we will use to interface with the ZNS device. It comes with an interface that allows to write to zones without worrying about SPDK internals. There are functions for reading lbas, writing lbas, erasing zones, getting write heads of zones and getting general device information.
It comes with a:
* shared library called `znsdevicelib`. To be used in our key-value store.
* standalone executable for CLI, run with `./bin/znsdevice`. To be used for debugging, e.g. resetting zones, reading from the device.
* set of unittests to ensure validity, run with `./bin/device_initial_test`. Automatically tested when CMake is used, except when turned off.

# How to build
There are multiple ways to built this project.
In principal only CMake and SPDK and its dependencies are needed. Please set the SPDK dir with an environment variable `SPDK_DIR`, otherwise the dir is not found.
Then it should compile (tested on Ubuntu 20.04 LTS), but no guarantees are made. For example:
```bash
export SPDK_DIR=/home/$USER/local/spdk
rm -f CMakeCache.txt
cmake .
make <TARGET>
```
if the output does not change, try cleaning the build with:
```bash
make clean
```
To further speed up compilation, we advise to use `ninja` instead of plain old `makefiles` and `mold` as a linker. For example:
```bash
rm -f CMakeCache.txt
cmake -j$(nproc) -G Ninja .
mold -run ninja build.ninja <TARGET>
# cleaning 
mold -run ninja build.ninja clean 
```