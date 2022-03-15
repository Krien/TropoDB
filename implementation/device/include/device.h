#ifndef LSM_ZNS_DEVICE_H
#define LSM_ZNS_DEVICE H

namespace ZnsDevice{
    extern "C" {
        struct Zone {

        };

        struct Completion {
            bool done = false;
        };
        
        struct ZnsDevice {

        };

        int
        open(const char* devicename);

        int
        append();

        int
        read();

        void *
        __reserve_dma(uint64_t size);

        int
        __get_block_alligned_size(uint64_t* size, uint64_t* blocks);

        static void
        __operation_complete(void *arg, const struct spdk_nvme_cpl *completion);

        static void
        __append_complete(void *arg, const struct spdk_nvme_cpl *completion);

        static void
        __read_complete(void *arg, const struct spdk_nvme_cpl *completion);


        int
        __probe();

        int
        __attach();
    }
}
#endif