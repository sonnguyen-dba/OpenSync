#include "TimeUtils.h"
#include "../logger/Logger.h"
#include "../external/date.h"
#include <iostream>
#include <cmath>
#include <cstring>
#include <cstdio>
#include <algorithm>
#include <unordered_map>

using namespace std;
using namespace std::chrono;
using namespace date;

namespace TimeUtils {

    static constexpr int64_t UNIX_AD1970_01_01 = 719527LL * 24 * 60 * 60;
    static constexpr int64_t UNIX_BC1970_01_01 = 718798LL * 24 * 60 * 60;
    static constexpr int64_t UNIX_AD9999_12_31 = 2932896LL * 24 * 60 * 60;
    static constexpr int64_t UNIX_BC4712_01_01 = -2440588LL * 24 * 60 * 60;

    static const int64_t cumDays[12] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    static const int64_t cumDaysLeap[12] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

    int64_t yearToDays(int year, int /*month*/) {
        return year * 365LL + year / 4 - year / 100 + year / 400;
    }

    int64_t yearToDaysBC(int year, int /*month*/) {
        return year * 365LL + year / 4 - year / 100 + year / 400;
    }

    bool parseTimezone(std::string str, int64_t& out) {
        // Sử dụng unordered_map để ánh xạ timezone nhanh hơn thay vì chuỗi if-else
        static const std::unordered_map<std::string, std::string> timezoneMap = {
            {"Etc/GMT-14", "-14:00"}, {"Etc/GMT-13", "-13:00"}, {"Etc/GMT-12", "-12:00"},
            {"Etc/GMT-11", "-11:00"}, {"HST", "-10:00"}, {"Etc/GMT-10", "-10:00"},
            {"Etc/GMT-9", "-09:00"}, {"PST", "-08:00"}, {"PST8PDT", "-08:00"},
            {"Etc/GMT-8", "-08:00"}, {"MST", "-07:00"}, {"MST7MDT", "-07:00"},
            {"Etc/GMT-7", "-07:00"}, {"Asia/Bangkok", "+07:00"}, // Giữ lại từ code cũ của bạn
            {"CST", "-06:00"}, {"CST6CDT", "-06:00"}, {"Etc/GMT-6", "-06:00"},
            {"EST", "-05:00"}, {"EST5EDT", "-05:00"}, {"Etc/GMT-5", "-05:00"},
            {"Etc/GMT-4", "-04:00"}, {"Etc/GMT-3", "-03:00"}, {"Etc/GMT-2", "-02:00"},
            {"Etc/GMT-1", "-01:00"}, {"GMT", "+00:00"}, {"Etc/GMT", "+00:00"},
            {"Greenwich", "+00:00"}, {"Etc/Greenwich", "+00:00"}, {"GMT0", "+00:00"},
            {"Etc/GMT0", "+00:00"}, {"GMT+0", "+00:00"}, {"Etc/GMT-0", "+00:00"},
            {"UTC", "+00:00"}, {"Etc/UTC", "+00:00"}, {"UCT", "+00:00"},
            {"Etc/UCT", "+00:00"}, {"Universal", "+00:00"}, {"Etc/Universal", "+00:00"},
            {"WET", "+00:00"}, {"MET", "+01:00"}, {"CET", "+01:00"},
            {"Etc/GMT+1", "+01:00"}, {"EET", "+02:00"}, {"Etc/GMT+2", "+02:00"},
            {"Etc/GMT+3", "+03:00"}, {"Etc/GMT+4", "+04:00"}, {"Etc/GMT+5", "+05:00"},
            {"Etc/GMT+6", "+06:00"}, {"Etc/GMT+7", "+07:00"}, {"PRC", "+08:00"},
            {"ROC", "+08:00"}, {"Etc/GMT+8", "+08:00"}, {"Etc/GMT+9", "+09:00"},
            {"Etc/GMT+10", "+10:00"}, {"Etc/GMT+11", "+11:00"}, {"Etc/GMT+12", "+12:00"}
        };

        // Kiểm tra xem str có trong map không
        auto it = timezoneMap.find(str);
        if (it != timezoneMap.end()) {
            str = it->second;
        }

        // Xử lý định dạng +HH:MM hoặc -HH:MM
        if (str.length() == 6 && (str[0] == '+' || str[0] == '-')) {
            int sign = (str[0] == '-') ? -1 : 1;
            if (str[3] != ':' || !isdigit(str[1]) || !isdigit(str[2]) ||
                !isdigit(str[4]) || !isdigit(str[5])) {
                out = 0;
                return false;
            }

            int hours = (str[1] - '0') * 10 + (str[2] - '0');
            int mins = (str[4] - '0') * 10 + (str[5] - '0');

            if (hours > 14 || mins > 59) {
                out = 0;
                return false;
            }

            out = sign * (hours * 3600 + mins * 60);
            return true;
        }

        out = 0;
        return false;
    }

    std::string timezoneToString(int64_t tz) {
        char result[9]; // đủ chứa "+HH:MM" + '\0'
        result[0] = (tz < 0) ? '-' : '+';
        if (tz < 0) tz = -tz;

        int64_t hours = tz / 3600;
        int64_t mins = (tz % 3600) / 60;

        snprintf(result + 1, sizeof(result) - 1, "%02d:%02d", static_cast<int>(hours), static_cast<int>(mins));

        return std::string(result);
    }

    int64_t valuesToEpoch(int year, int month, int day, int hour, int minute, int second, int tz) {
        int64_t result;
        if (year > 0) {
            result = yearToDays(year, month) + cumDays[(month - 1) % 12] + (day - 1);
            result *= 24;
            result += hour;
            result *= 60;
            result += minute;
            result *= 60;
            result += second;
            return result - UNIX_AD1970_01_01 - tz;
        } else {
            result = -yearToDaysBC(-year, month) + cumDays[(month - 1) % 12] + (day - 1);
            result *= 24;
            result += hour;
            result *= 60;
            result += minute;
            result *= 60;
            result += second;
            return result - UNIX_BC1970_01_01 - tz;
        }
    }

  std::string convertTimestamp(int64_t value, int unit) {
    try {
        int64_t timestamp_us = value;
        std::string unit_str;

        switch (unit) {
            case 0: // Nanosecond
                unit_str = "nanoseconds";
                timestamp_us = value / 1000; // Nanosecond to microsecond
                Logger::debug("Converted timestamp from nanoseconds to microseconds: value=" +
                             std::to_string(value) + ", result=" + std::to_string(timestamp_us));
                break;
            case 1: // Microsecond
                unit_str = "microseconds";
                timestamp_us = value; // Giữ nguyên
                Logger::debug("Interpreted timestamp as microseconds: value=" + std::to_string(value));
                break;
            case 2: // Millisecond
                unit_str = "milliseconds";
                timestamp_us = value * 1000; // Millisecond to microsecond
                Logger::debug("Converted timestamp from milliseconds to microseconds: value=" +
                             std::to_string(value) + ", result=" + std::to_string(timestamp_us));
                break;
            default:
                Logger::error("Invalid timestamp unit: " + std::to_string(unit) + ", treating as microseconds");
                unit_str = "microseconds";
                timestamp_us = value;
                break;
        }

        // Kiểm tra phạm vi (1850-01-01 to 2100-01-01)
        const int64_t MIN_TIMESTAMP_US = -3786825600000000; // 1850-01-01
        const int64_t MAX_TIMESTAMP_US = 4102444800000000;  // 2100-01-01
        if (timestamp_us < MIN_TIMESTAMP_US || timestamp_us > MAX_TIMESTAMP_US) {
            Logger::warn("Timestamp out of range: " + std::to_string(timestamp_us) + " (" + unit_str + ")");
            return "INVALID_TIMESTAMP";
        }

        return convertMicrosecondsToTimestamp(timestamp_us);
    } catch (const std::exception& e) {
        Logger::error("[TimeUtils] Timestamp conversion failed: " + std::string(e.what()));
        return "INVALID_TIMESTAMP";
    }
 }
  std::string convertMicrosecondsToTimestamp(int64_t microseconds) {
    try {
        auto dur = std::chrono::microseconds(microseconds);
        auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>(dur);

        std::ostringstream oss;
        oss << date::format("%F %T", tp);  // yyyy-MM-dd HH:mm:ss
        return oss.str();
    } catch (const std::exception& e) {
        std::cerr << "[TimeUtils] Timestamp conversion failed: " << e.what() << std::endl;
        return "INVALID_TIMESTAMP";
    }
  }

   std::string convertMicrosecondsToDate(int64_t microseconds) {
    try {
        auto dur = std::chrono::microseconds(microseconds);
        auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>(dur);

        sys_days dp = floor<days>(tp);
        year_month_day ymd(dp);

        std::ostringstream oss;
        oss << ymd;  // yyyy-MM-dd
        return oss.str();
    } catch (const std::exception& e) {
        std::cerr << "[TimeUtils] Date conversion failed: " << e.what() << std::endl;
        return "INVALID_DATE";
    }
  }

  std::string epochToIso8601(int64_t microseconds) {
    try {
        auto dur = std::chrono::microseconds(microseconds);
        auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>(dur);

        std::ostringstream oss;
        oss << date::format("%FT%T", tp);  // YYYY-MM-DDTHH:MM:SS
        int64_t us_part = microseconds % 1000000;
        if (us_part < 0) us_part += 1000000;

        // more .FFFFFF
        char fractional[16];
        snprintf(fractional, sizeof(fractional), ".%06ld", us_part);
        oss << fractional;

        oss << "Z";  // UTC
        return oss.str();
    } catch (const std::exception& e) {
        std::cerr << "[TimeUtils] ISO 8601 conversion failed: " << e.what() << std::endl;
        return "INVALID_ISO8601";
    }
  }
} // namespace TimeUtils
