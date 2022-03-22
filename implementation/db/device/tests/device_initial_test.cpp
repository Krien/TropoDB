#include "device.h"
#include "utils.h"
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

int write_pattern(char** pattern, ZnsDevice::QPair* qpair, int32_t size, int32_t jump) {
    if (*pattern != NULL) {
        ZnsDevice::z_free(qpair, *pattern);
    }
    *pattern = (char*)ZnsDevice::z_calloc(qpair, size, sizeof(char*));
    if(*pattern == NULL) {
        return 1;
    }
    for (int j=0; j<size;j++) {
        (*pattern)[j] = j % 200 + jump;
    }
    return 0;
}

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
    assert((*manager)->info.lba_cap > 0);

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
    printf("lba_cap is %d\n", info.lba_cap);

    uint64_t write_head;
    printf("----------------------WORKLOAD SMALL----------------------\n");
    // make space by resetting the device zones
    rc = ZnsDevice::z_reset(*qpair, 0, true);
    DEBUG_TEST_PRINT("reset all code ", rc);
    VALID(rc); 
    rc = ZnsDevice::z_get_zone_head(*qpair, 0, &write_head);
    VALID(rc);
    assert(write_head == 0);
    char** pattern_1 = (char**)calloc(1, sizeof(char**));
    rc = write_pattern(pattern_1, *qpair, info.lba_size, 10);
    VALID(rc);
    rc = ZnsDevice::z_append(*qpair, 0, *pattern_1, info.lba_size);
    DEBUG_TEST_PRINT("append alligned ", rc);
    VALID(rc);
    rc = ZnsDevice::z_get_zone_head(*qpair, 0, &write_head);
    VALID(rc);
    assert(write_head == 1);
    char** pattern_2 = (char**)calloc(1, sizeof(char**));
    rc = write_pattern(pattern_2, *qpair, info.zasl, 13);
    VALID(rc);
    rc = ZnsDevice::z_append(*qpair, 0, *pattern_2, info.zasl);
    DEBUG_TEST_PRINT("append zasl ", rc);
    VALID(rc);
    rc = ZnsDevice::z_get_zone_head(*qpair, 0, &write_head);
    VALID(rc);
    assert(write_head == 1+info.zasl/info.lba_size);
    char* pattern_read_1 = (char*)ZnsDevice::z_calloc(*qpair, info.lba_size, sizeof(char*));
    rc = ZnsDevice::z_read(*qpair, 0, pattern_read_1, info.lba_size);
    DEBUG_TEST_PRINT("read alligned ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size; i++) {
        assert((char*)(pattern_read_1)[i] == (char*)(*pattern_1)[i]);
    }
    char* pattern_read_2 = (char*)ZnsDevice::z_calloc(*qpair, info.zasl, sizeof(char*));
    rc = ZnsDevice::z_read(*qpair, 1, pattern_read_2, info.zasl);
    DEBUG_TEST_PRINT("read zasl ", rc);
    VALID(rc);
    for(int i=0; i<info.zasl; i++) {
        assert((char*)(pattern_read_2)[i] == (char*)(*pattern_2)[i]);
    }
    rc = ZnsDevice::z_reset(*qpair, 0, true);
    DEBUG_TEST_PRINT("reset all ", rc);
    VALID(rc);
    rc = ZnsDevice::z_read(*qpair, 1, pattern_read_2, info.zasl);
    DEBUG_TEST_PRINT("verify empty first zone ", rc);
    VALID(rc);
    for(int i=0; i<info.zasl; i++) {
        assert((char*)(pattern_read_2)[i] == 0);
    }

    printf("----------------------WORKLOAD FILL----------------------\n");
    char** pattern_3 = (char**)calloc(1, sizeof(char**));
    rc = write_pattern(pattern_3, *qpair, info.lba_size*info.lba_cap, 19);
    VALID(rc);
    rc = ZnsDevice::z_append(*qpair, 0, *pattern_3, info.lba_size*info.lba_cap);
    DEBUG_TEST_PRINT("fill entire device ", rc);
    VALID(rc);
    for (int i=0; i<info.lba_cap/info.zone_size;i++) {
        rc = ZnsDevice::z_get_zone_head(*qpair, i*info.zone_size, &write_head);
        VALID(rc);
        assert(write_head == ~0lu);
    }
    char* pattern_read_3 = (char*)ZnsDevice::z_calloc(*qpair, info.lba_size*info.lba_cap, sizeof(char*));
    rc = ZnsDevice::z_read(*qpair, 0, pattern_read_3, info.lba_size*info.lba_cap);
    DEBUG_TEST_PRINT("read entire device ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size*info.lba_cap; i++) {
        assert((char*)(pattern_read_3)[i] == (char*)(*pattern_3)[i]);
    }
    rc = ZnsDevice::z_reset(*qpair, info.zone_size, false);
    rc = ZnsDevice::z_reset(*qpair, info.zone_size*2, false) | rc;
    DEBUG_TEST_PRINT("reset zone 2,3 ", rc);
    VALID(rc);
    rc = ZnsDevice::z_get_zone_head(*qpair, 0, &write_head);
    VALID(rc);
    assert(write_head == ~0lu);
    rc = ZnsDevice::z_get_zone_head(*qpair, info.zone_size, &write_head);
    VALID(rc);
    assert(write_head == info.zone_size);
    rc = ZnsDevice::z_get_zone_head(*qpair, info.zone_size*2, &write_head);
    VALID(rc);
    assert(write_head == info.zone_size * 2);
    char* pattern_read_4 = (char*)ZnsDevice::z_calloc(*qpair, info.lba_size*info.zone_size, sizeof(char*));
    rc = ZnsDevice::z_read(*qpair, 0, pattern_read_4, info.lba_size*info.zone_size);
    DEBUG_TEST_PRINT("read zone 1 ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size*info.zone_size; i++) {
        assert((char*)(pattern_read_4)[i] == (char*)(*pattern_3)[i]);
    }
    rc = ZnsDevice::z_read(*qpair, info.zone_size, pattern_read_4, info.lba_size*info.zone_size);
    DEBUG_TEST_PRINT("read zone 2 ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size*info.zone_size; i++) {
        assert((char*)(pattern_read_4)[i] == 0);
    }
    rc = ZnsDevice::z_read(*qpair, info.zone_size*2, pattern_read_4, info.lba_size*info.zone_size);
    DEBUG_TEST_PRINT("read zone 3 ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size*info.zone_size; i++) {
        assert((char*)(pattern_read_4)[i] == 0);
    }
    rc = ZnsDevice::z_read(*qpair, info.zone_size*3, pattern_read_4, info.lba_size*info.zone_size);
    DEBUG_TEST_PRINT("read zone 4 ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size*info.zone_size; i++) {
        assert((char*)(pattern_read_4)[i] == (char*)(*pattern_3)[i+info.zone_size*3*info.lba_size]);
    }
    rc = ZnsDevice::z_reset(*qpair, 0, true);
    DEBUG_TEST_PRINT("reset all ", rc);
    VALID(rc);

    printf("----------------------WORKLOAD EDGE----------------------\n");
    rc = ZnsDevice::z_append(*qpair, 0, *pattern_3, info.lba_size*(info.zone_size-3));
    DEBUG_TEST_PRINT("zone friction part 1: append 1 zoneborder - 3 ", rc);
    VALID(rc);
    rc = ZnsDevice::z_get_zone_head(*qpair, 0, &write_head);
    VALID(rc);
    assert(write_head == info.zone_size - 3);
    rc = ZnsDevice::z_append(*qpair, info.zone_size-3, *pattern_3 + info.lba_size*(info.zone_size-3), info.lba_size*6);
    DEBUG_TEST_PRINT("zone friction part 2: append 1 zoneborder + 6 ", rc);
    VALID(rc);
    rc = ZnsDevice::z_get_zone_head(*qpair, 0, &write_head);
    VALID(rc);
    assert(write_head == ~0lu);
    rc = ZnsDevice::z_get_zone_head(*qpair, info.zone_size, &write_head);
    VALID(rc);
    assert(write_head == info.zone_size + 3);
    rc = ZnsDevice::z_append(*qpair, info.zone_size+3, *pattern_3 + info.lba_size*(info.zone_size+3), info.lba_size*13);
    DEBUG_TEST_PRINT("zone friction part 3: append 1 zoneborder + 16 ", rc);
    VALID(rc);
    rc = ZnsDevice::z_get_zone_head(*qpair, info.zone_size, &write_head);
    VALID(rc);
    assert(write_head == info.zone_size + 16);
    rc = ZnsDevice::z_read(*qpair, 0, pattern_read_4, info.lba_size*(info.zone_size-3));
    DEBUG_TEST_PRINT("zone friction part 4: read 1 zoneborder - 3 ", rc);
    VALID(rc);
    rc = ZnsDevice::z_read(*qpair, info.zone_size-3, pattern_read_4 + info.lba_size*(info.zone_size-3), info.lba_size*6);
    DEBUG_TEST_PRINT("zone friction part 5: read 1 zoneborder + 3 ", rc);
    VALID(rc);
    rc = ZnsDevice::z_read(*qpair, info.zone_size+3, pattern_read_4 + info.lba_size*(info.zone_size+3), info.lba_size*13);
    DEBUG_TEST_PRINT("zone friction part 6: read 1 zoneborder + 16 ", rc);
    VALID(rc);
    for(int i=0; i<info.lba_size*(info.zone_size+15); i++) {
        assert((char*)(pattern_read_4)[i] == (char*)(*pattern_3)[i]);
    }

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
    free(pattern_1);
    free(pattern_2);
    free(pattern_3);

    free(qpair);
    free(manager);
}
}
