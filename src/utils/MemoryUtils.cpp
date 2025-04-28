#include "MemoryUtils.h"
#include <fstream>
#include <string>
#include <unistd.h>

std::pair<size_t, size_t> MemoryUtils::getMemoryUsageMB() {
    std::ifstream statusFile("/proc/self/status");
    std::string line;
    size_t vmSizeKB = 0;
    size_t rssKB = 0;

    while (std::getline(statusFile, line)) {
        if (line.find("VmSize:") == 0) {
            sscanf(line.c_str(), "VmSize: %lu kB", &vmSizeKB);
        } else if (line.find("VmRSS:") == 0) {
            sscanf(line.c_str(), "VmRSS: %lu kB", &rssKB);
        }
    }

    return {vmSizeKB / 1024, rssKB / 1024}; // Convert to MB
}
