/*
 * MIT License
Copyright (c) 2021 - current
Authors:  Animesh Trivedi
This code is part of the Storage System Course at VU Amsterdam
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include <cstdio>
#include <cstdlib>
#include <cerrno>

#include <cstring>
#include <cassert>

#include "device.h"

extern "C" {

int main(int argc, char **argv)
{
    struct device_manager *device;
    struct device_manager **devices = (struct device_manager **)calloc(1, sizeof(struct device_manager *));
    int ret = ss_init(devices);
    device = devices[0];
    if (ret != 0) {
        printf("Error while initing, check that a device is binded.\n");
        return ret;
    }

    ret = count_and_show_all_nvme_devices(device);
    if(ret < 1){
        printf("the host device scans failed, %d \n", ret);
        return ret;
    }

    struct ns_entry** ns = (struct ns_entry**)calloc(1, sizeof(struct ns_entry));
    ret = ss_open_device(ns, "zns-dev             QEMU NVMe Ctrl                          1.0");
    if (ret != 0) {
        printf("Device not found \n");
        return ret;
    }

    uint32_t lba_size;
    ret = ss_get_lba_size(*ns,  &lba_size);
    if (ret != 0) {
        printf("Error on getting lba \n");
        return ret;
    }

    struct zone_to_test ztest{};
    ztest.lba_size_in_use = lba_size;

    ret = ss_zns_device_zone_reset(*ns,0);
    printf("reset status %d\n", ret);

    char* data = (char*)calloc(512*4096*2, sizeof(char));
    for (int j=0; j<512*4096*2;j++) {
        data[j] = j % 200 + 10;
    }
    char* mata = (char*)calloc(512*4096*2, sizeof(char));
    for (int j=0; j<512*4096*2;j++) {
        mata[j] = j % 200 + 15;
    }
    printf("Filled test buffer\n");
    ret = ss_zns_device_zone_append(*ns, 0, 0, data, 512*4096*1, NULL);
    ret = ss_zns_device_zone_append(*ns, 0, 0, mata, 512*4096*1, NULL);
    printf("write status %d\n", ret);

    char* zata = (char*)calloc(512*4096*6, sizeof(char));
    ret = ss_nvme_device_read(*ns, 0, 0, zata, 512*4096*2);

    for (int j=0; j<512*4096; j++) {
        if(data[j%(512*4096*1)] != zata[j]) {
            printf("NEQ %d\n", j);
            exit(1);
        }
    }
    for (int j=0; j<512*4096; j++) {
        if(mata[j%(512*4096*1)] != zata[j+512*4096]) {
            printf("NEQ %d\n", j);
            exit(1);
        }
    }
    free(data);
    free(zata);
    free(mata);
    printf("Data is equal\n");
    free(ns);
    ss_cleanup(*devices);
    free(devices);
    return 0;
}
}
