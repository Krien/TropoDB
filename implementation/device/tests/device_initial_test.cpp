#include "device.h"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

extern "C" {

#define DEBUG
#ifdef DEBUG
    #define DEBUG_TEST_PRINT(str, code)  do{if ((code) == 0)    \
            {printf("%s\x1B[32m%u\x1B[0m\n", (str), (code));}   \
        else                                                    \
            {printf("%s\x1B[31m%u\x1B[0m\n", (str), (code));}   \
    }while(0)
#else 
    #define DEBUG_TEST_PRINT(str, code) do{}while(0)
#endif
 
#define VALID(rc) assert((rc) == 0)
#define INVALID(rc) assert((rc) != 0)

int main(int argc, char **argv) {
    printf("----------------------UNDEFINED----------------------\n");
    int rc = ZnsDevice::__TEST_interface();
    DEBUG_TEST_PRINT("interface setup ", rc);
    assert(rc == 2022);

    // init spdk
    printf("----------------------INIT----------------------\n");
    ZnsDevice::DeviceManager **manager = (ZnsDevice::DeviceManager**)calloc(1, sizeof(ZnsDevice::DeviceManager));
    rc = ZnsDevice::z_init(manager);
    VALID(rc);

    // try non-existent device
    rc = ZnsDevice::z_open(*manager, "non-existent traddr");
    DEBUG_TEST_PRINT("non-existent return code ", rc);
    INVALID(rc);

    // try existing device
    rc = ZnsDevice::z_open(*manager, "0000:00:04.0");
    DEBUG_TEST_PRINT("existing return code ", rc);
    VALID(rc);

    // ensure that everything from this device is OK
    assert((*manager)->ctrlr != NULL);
    assert((*manager)->ns != NULL);
    assert((*manager)->info.lba_size > 0);
    assert((*manager)->info.mdts > 0);
    assert((*manager)->info.zasl > 0);
    assert((*manager)->info.zone_size > 0);

    // create qpair
    ZnsDevice::QPair **qpair = (ZnsDevice::QPair**)calloc(1, sizeof(ZnsDevice::QPair*));
    rc = ZnsDevice::z_create_qpair(*manager, qpair);
    DEBUG_TEST_PRINT("Qpair creation code ", rc);
    VALID(rc);
    assert(qpair != nullptr);

    // get and verify data (based on ZNS QEMU image)
    ZnsDevice::DeviceInfo info = {};
    rc = ZnsDevice::z_get_device_info(&info, *manager);
    DEBUG_TEST_PRINT("get info code ", rc);
    VALID(rc); 
    printf("lba size is %d\n", info.lba_size);
    printf("zone size is %d\n", info.zone_size);
    printf("mdts is %d\n", info.mdts);
    printf("zasl is %d\n", info.zasl);

    printf("----------------------WORKLOAD BASIC----------------------\n");
    // make space by resetting the device zones
    rc = ZnsDevice::z_reset(*qpair, 0, true);
    DEBUG_TEST_PRINT("reset all code ", rc);
    VALID(rc); 

    // destroy qpair
    printf("----------------------CLOSE----------------------\n");
    rc = ZnsDevice::z_destroy_qpair(*qpair);
    DEBUG_TEST_PRINT("valid destroy code ", rc);
    VALID(rc); 

    // close device
    rc = ZnsDevice::z_close(*manager);
    DEBUG_TEST_PRINT("valid close code ", rc);
    VALID(rc); 

    // can not close twice
    rc = ZnsDevice::z_close(*manager);
    DEBUG_TEST_PRINT("invalid close code ", rc);
    INVALID(rc); 

    rc = ZnsDevice::z_shutdown(*manager);
    DEBUG_TEST_PRINT("valid shutdown code ", rc);
    VALID(rc); 

    // cleanup local
    free(qpair);
    free(manager);
}
}
