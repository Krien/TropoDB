#include "device.h"

int main(int argc, char **argv) {
    ZnsDevice::DeviceManager **manager = (ZnsDevice::DeviceManager**)calloc(1, sizeof(ZnsDevice::DeviceManager));
    ZnsDevice::z_open(manager, "non-existent device");
}
