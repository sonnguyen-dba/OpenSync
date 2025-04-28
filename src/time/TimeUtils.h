#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#include <string>
#include <cstdint>
#include <ctime>

namespace TimeUtils {

    std::string convertTimestamp(int64_t value, int unit); // Xử lý nanosecond, microsecond, millisecond

    std::string convertMicrosecondsToTimestamp(int64_t microseconds);
    std::string convertMicrosecondsToDate(int64_t microseconds);

    // Trả về định dạng ISO 8601 với microsecond và UTC (Z)
    std::string epochToIso8601(int64_t microseconds);

    // Parse timezone string (e.g. "+07:00", "GMT", "UTC") to seconds offset
    bool parseTimezone(std::string str, int64_t& out);

    // Convert timezone seconds to string representation (+07:00, -05:00)
    std::string timezoneToString(int64_t tz);

    // Convert date-time to epoch (relative to 1970-01-01 UTC)
    int64_t valuesToEpoch(int year, int month, int day, int hour, int minute, int second, int tz);

    // Format epoch time to ISO8601 string
    //uint64_t epochToIso8601(time_t timestamp, char* buffer, bool addT = false, bool addZ = false);

    // Year-to-days helper (AD)
    int64_t yearToDays(int year, int month);

    // Year-to-days helper (BC)
    int64_t yearToDaysBC(int year, int month);




}

#endif // TIME_UTILS_H

