#include "TimeUtils.h"
#include "../logger/Logger.h"
#include "../common/date.h"
#include "../utils/SQLUtils.h"
#include <iostream>
#include <cmath>
#include <cstring>
#include <cstdio>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <regex>
#include <ctime>
#include <iomanip>
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

/*    std::string convertMicrosecondsToTimestamp(int64_t microseconds) {
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

            // Thêm phần .FFFFFF
            char fractional[16];
            snprintf(fractional, sizeof(fractional), ".%06ld", us_part);
            oss << fractional;

            oss << "Z";  // UTC
            return oss.str();
        } catch (const std::exception& e) {
            std::cerr << "[TimeUtils] ISO 8601 conversion failed: " << e.what() << std::endl;
            return "INVALID_ISO8601";
        }
    }*/


  std::string convertTimestamp(int64_t value, int unit) {
    try {
        int64_t timestamp_us = value;
        std::string unit_str;

        switch (unit) {
            case 0: // Nanosecond
                unit_str = "nanoseconds";
                timestamp_us = value / 1000; // Nanosecond to microsecond
                OpenSync::Logger::debug("Converted timestamp from nanoseconds to microseconds: value=" +
                             std::to_string(value) + ", result=" + std::to_string(timestamp_us));
                break;
            case 1: // Microsecond
                unit_str = "microseconds";
                timestamp_us = value; // Giữ nguyên
                OpenSync::Logger::debug("Interpreted timestamp as microseconds: value=" + std::to_string(value));
                break;
            case 2: // Millisecond
                unit_str = "milliseconds";
                timestamp_us = value * 1000; // Millisecond to microsecond
                OpenSync::Logger::debug("Converted timestamp from milliseconds to microseconds: value=" +
                             std::to_string(value) + ", result=" + std::to_string(timestamp_us));
                break;
            default:
                OpenSync::Logger::error("Invalid timestamp unit: " + std::to_string(unit) + ", treating as microseconds");
                unit_str = "microseconds";
                timestamp_us = value;
                break;
        }

        // Kiểm tra phạm vi (1850-01-01 to 2100-01-01)
        const int64_t MIN_TIMESTAMP_US = -3786825600000000; // 1850-01-01
        const int64_t MAX_TIMESTAMP_US = 4102444800000000;  // 2100-01-01
        if (timestamp_us < MIN_TIMESTAMP_US || timestamp_us > MAX_TIMESTAMP_US) {
            OpenSync::Logger::warn("Timestamp out of range: " + std::to_string(timestamp_us) + " (" + unit_str + ")");
            return "INVALID_TIMESTAMP";
        }

        return convertMicrosecondsToTimestamp(timestamp_us);
    } catch (const std::exception& e) {
        OpenSync::Logger::error("[TimeUtils] Timestamp conversion failed: " + std::string(e.what()));
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

        // Thêm phần .FFFFFF
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

  /*
  int64_t parseOracleTimestampToMicroseconds(const std::string& ts) {
    // Ví dụ input: "19-MAY-25 09.25.24 PM +07:00"
    std::tm tm = {};
    int hour = 0, min = 0, sec = 0;
    char monthStr[4], ampm[3], tz[7];

    if (sscanf(ts.c_str(), "%2d-%3s-%2d %2d.%2d.%2d %2s %6s",
               &tm.tm_mday, monthStr, &tm.tm_year,
               &hour, &min, &sec, ampm, tz) != 8) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp: " + ts);
        return 0;
    }

    // Chuyển monthStr sang tháng
    std::unordered_map<std::string, int> monthMap = {
        {"JAN", 0}, {"FEB", 1}, {"MAR", 2}, {"APR", 3}, {"MAY", 4},
        {"JUN", 5}, {"JUL", 6}, {"AUG", 7}, {"SEP", 8}, {"OCT", 9},
        {"NOV", 10}, {"DEC", 11}
    };
    std::string monthUpper = SQLUtils::toUpper(std::string(monthStr));
    tm.tm_mon = monthMap[monthUpper];
    tm.tm_year += 100; // Oracle trả "25" → 2025

    // Convert AM/PM
    if (strcmp(ampm, "PM") == 0 && hour < 12) hour += 12;
    if (strcmp(ampm, "AM") == 0 && hour == 12) hour = 0;

    tm.tm_hour = hour;
    tm.tm_min = min;
    tm.tm_sec = sec;
    tm.tm_isdst = -1;

    // Convert to epoch (localtime)
    time_t epoch_sec = mktime(&tm);
    return static_cast<int64_t>(epoch_sec) * 1'000'000;  // microseconds
}*/


/*int64_t TimeUtils::parseOracleTimestampToMicroseconds(const std::string& ts) {
    try {
        if (ts.empty() || ts == "NULL") return 0;

        int day = 0, year = 0, hour = 0, minute = 0, second = 0;
        char monthStr[4] = {0}, ampm[3] = {0}, tz[10] = {0};

        // Format ví dụ: "19-MAY-25 09.25.24 PM +07:00"
        int parsed = sscanf(ts.c_str(), "%2d-%3s-%2d %2d.%2d.%2d %2s %9s",
                            &day, monthStr, &year, &hour, &minute, &second, ampm, tz);

        if (parsed < 7) {
            OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp: " + ts);
            return 0;
        }

        // Chuẩn hóa AM/PM
        std::string ampmStr = SQLUtils::toUpper(std::string(ampm));
        if (ampmStr == "PM" && hour < 12) hour += 12;
        if (ampmStr == "AM" && hour == 12) hour = 0;

        // Convert month string to number
        std::string monthUpper = SQLUtils::toUpper(std::string(monthStr));
        std::unordered_map<std::string, int> monthMap = {
            {"JAN", 1}, {"FEB", 2}, {"MAR", 3}, {"APR", 4}, {"MAY", 5}, {"JUN", 6},
            {"JUL", 7}, {"AUG", 8}, {"SEP", 9}, {"OCT", 10}, {"NOV", 11}, {"DEC", 12}
        };

        int month = monthMap.count(monthUpper) ? monthMap[monthUpper] : 1;

        // Oracle-style 2-digit year logic
        if (year < 100) {
            if (year <= 49) year += 2000;
            else year += 1900;
        }

        // Parse timezone offset
        int64_t tzOffset = 0;
        if (!parseTimezone(std::string(tz), tzOffset)) {
            tzOffset = 0;
        }

        int64_t epochSec = valuesToEpoch(year, month, day, hour, minute, second, tzOffset);
        int64_t microseconds = epochSec * 1000000LL;

        OpenSync::Logger::debug("🕒 Parsed Oracle timestamp: " + ts +
            " → epoch(us)=" + std::to_string(microseconds));

        return microseconds;

    } catch (const std::exception& ex) {
        OpenSync::Logger::warn("❌ Exception while parsing Oracle timestamp: " + ts + " → " + ex.what());
        return 0;
    }
}*/


int64_t parseOracleTimestampToMicroseconds(const std::string& ts) {
    if (ts.empty() || ts == "NULL" || ts == "0") {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (empty or null): " + ts);
        return 0;
    }

    // Regex để khớp định dạng Oracle timestamp
    std::regex tsRegex(R"(^(\d{2})-([A-Z]{3})-(\d{2,4})(?:\s+(\d{2})\.(\d{2})\.(\d{2})(?:\.(\d{1,6}))?\s*(AM|PM)?\s*([+-]\d{2}:\d{2})?)?$)");
    std::smatch match;

    if (!std::regex_match(ts, match, tsRegex)) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (invalid format): " + ts);
        return 0;
    }

    // Lấy các thành phần
    std::string dayStr = match[1].str();
    std::string monthStr = match[2].str();
    std::string yearStr = match[3].str();
    std::string hourStr = match[4].str();
    std::string minuteStr = match[5].str();
    std::string secondStr = match[6].str();
    std::string microStr = match[7].str();
    std::string ampm = match[8].str();
    std::string tz = match[9].str();

    // Month map
    static std::unordered_map<std::string, int> monthMap = {
        {"JAN", 0}, {"FEB", 1}, {"MAR", 2}, {"APR", 3}, {"MAY", 4}, {"JUN", 5},
        {"JUL", 6}, {"AUG", 7}, {"SEP", 8}, {"OCT", 9}, {"NOV", 10}, {"DEC", 11}
    };

    // Chuyển đổi month
    std::string monthUpper = SQLUtils::toUpper(monthStr);
    if (monthMap.find(monthUpper) == monthMap.end()) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (invalid month): " + ts);
        return 0;
    }
    int month = monthMap[monthUpper];

    // Chuyển đổi year
    int year = std::stoi(yearStr);
    if (year < 100) {
        year += 1900;  // Luôn hiểu năm 2 chữ số thuộc thế kỷ 20
    }

    // Chuyển đổi day
    int day = std::stoi(dayStr);

    // Khởi tạo thời gian mặc định
    int hour = 0, minute = 0, second = 0, micros = 0;
    if (!hourStr.empty()) {
        hour = std::stoi(hourStr);
        minute = std::stoi(minuteStr);
        second = std::stoi(secondStr);

        // Xử lý AM/PM
        if (!ampm.empty()) {
            if (SQLUtils::toUpper(ampm) == "PM" && hour != 12) hour += 12;
            if (SQLUtils::toUpper(ampm) == "AM" && hour == 12) hour = 0;
        }

        // Xử lý micro giây
        if (!microStr.empty()) {
            micros = std::stoi(microStr);
            while (microStr.length() < 6) microStr += "0";
            micros = std::stoi(microStr);
        }
    }

    // Xây dựng struct tm
    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month;
    t.tm_mday = day;
    t.tm_hour = hour;
    t.tm_min = minute;
    t.tm_sec = second;

    // Chuyển đổi sang epoch time (UTC)
    time_t epoch_sec = timegm(&t);
    if (epoch_sec == -1) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (invalid timegm): " + ts);
        return 0;
    }

    // Xử lý múi giờ
    int64_t tzOffsetMicros = 0;
    if (!tz.empty()) {
        int tzHour, tzMin;
        char sign;
        std::istringstream tzStream(tz);
        tzStream >> sign >> tzHour >> tzMin;
        tzMin = tzMin * 100 / 60;  // Chuyển đổi phút
        int tzOffsetSec = tzHour * 3600 + tzMin * 60;
        if (sign == '-') tzOffsetSec = -tzOffsetSec;
        tzOffsetMicros = static_cast<int64_t>(tzOffsetSec) * 1000000;
    }

    // Tính micro giây cuối cùng
    int64_t result = static_cast<int64_t>(epoch_sec) * 1000000 + micros - tzOffsetMicros;
    OpenSync::Logger::debug("Parsed timestamp: " + ts + " -> " + std::to_string(result) + " microseconds");
    return result;
}

/*int64_t parseOracleTimestampToMicroseconds(const std::string& ts) {
    if (ts.empty() || ts == "NULL" || ts == "0") {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (empty or null): " + ts);
        return 0;
    }

    // Sample: 19-MAY-25 09.25.24 PM +07:00
    // Fallback: 09-SEP-00 or 20-APR-37 12.00.00 AM +07:00
    std::string datePart, timePart, ampm, tzPart;
    std::istringstream iss(ts);
    iss >> datePart >> timePart >> ampm >> tzPart;

    if (datePart.empty() || timePart.empty() || ampm.empty()) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (incomplete): " + ts);
        return 0;
    }

    std::string dayStr = datePart.substr(0, 2);
    std::string monthStr = datePart.substr(3, 3);
    std::string yearStr = datePart.substr(7);  // 2-digit or 4-digit

    // Month map
    static std::unordered_map<std::string, int> monthMap = {
        {"JAN", 0}, {"FEB", 1}, {"MAR", 2}, {"APR", 3}, {"MAY", 4}, {"JUN", 5},
        {"JUL", 6}, {"AUG", 7}, {"SEP", 8}, {"OCT", 9}, {"NOV", 10}, {"DEC", 11}
    };

    int day = std::stoi(dayStr);
    std::string monthUpper = SQLUtils::toUpper(monthStr);
    int month = monthMap.count(monthUpper) ? monthMap[monthUpper] : -1;
    if (month < 0) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (invalid month): " + ts);
        return 0;
    }

    int year = std::stoi(yearStr);
    if (year < 100) {
        year += (year >= 50) ? 1900 : 2000;  // Oracle style
    }

    int hour = 0, minute = 0, second = 0;
    sscanf(timePart.c_str(), "%d.%d.%d", &hour, &minute, &second);

    if (SQLUtils::toUpper(ampm) == "PM" && hour != 12) hour += 12;
    if (SQLUtils::toUpper(ampm) == "AM" && hour == 12) hour = 0;

    // Build struct tm in UTC
    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon  = month;
    t.tm_mday = day;
    t.tm_hour = hour;
    t.tm_min  = minute;
    t.tm_sec  = second;

    time_t epoch_sec = timegm(&t);  // Convert to UTC
    if (epoch_sec < 0) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (invalid timegm): " + ts);
        return 0;
    }

    int64_t micros = static_cast<int64_t>(epoch_sec) * 1000000;
    return micros;
}*/

/*
int64_t parseOracleTimestampToMicroseconds(const std::string& ts) {
    if (ts.empty()) return 0;

    try {
        // Ví dụ: "20-APR-37 12.00.00 AM +07:00"
        std::string datetimePart = ts.substr(0, ts.find_first_of("+-", 10)); // "20-APR-37 12.00.00 AM"
        std::istringstream ss(datetimePart);

        std::string dayStr, monthStr, yearStr, timeStr, ampm;
        ss >> dayStr >> monthStr >> yearStr >> timeStr >> ampm;

        if (dayStr.empty() || monthStr.empty() || yearStr.empty() || timeStr.empty()) {
            OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (incomplete): " + ts);
            return 0;
        }

        int day = std::stoi(dayStr);
        int yy = std::stoi(yearStr);

        // ✅ Giải quyết năm 2 chữ số → ưu tiên thế kỷ 1900
        int year = 1900 + yy;

        // ✅ Chuẩn hóa tháng → số
        std::string monthUpper = SQLUtils::toUpper(monthStr);
        std::map<std::string, int> monthMap = {
            {"JAN", 1}, {"FEB", 2}, {"MAR", 3}, {"APR", 4}, {"MAY", 5}, {"JUN", 6},
            {"JUL", 7}, {"AUG", 8}, {"SEP", 9}, {"OCT", 10}, {"NOV", 11}, {"DEC", 12}
        };
        if (monthMap.find(monthUpper) == monthMap.end()) {
            OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp (invalid month): " + ts);
            return 0;
        }
        int month = monthMap[monthUpper];

        int hour = 0, minute = 0, second = 0;
        sscanf(timeStr.c_str(), "%d.%d.%d", &hour, &minute, &second);

        if (SQLUtils::toUpper(ampm) == "PM" && hour < 12) hour += 12;
        if (SQLUtils::toUpper(ampm) == "AM" && hour == 12) hour = 0;

        std::tm tmTime = {};
        tmTime.tm_year = year - 1900;
        tmTime.tm_mon  = month - 1;
        tmTime.tm_mday = day;
        tmTime.tm_hour = hour;
        tmTime.tm_min  = minute;
        tmTime.tm_sec  = second;
        tmTime.tm_isdst = -1;

        time_t epoch = timegm(&tmTime);  // UTC
        if (epoch < 0) {
            OpenSync::Logger::warn("❗ Invalid parsed date: " + ts);
            return 0;
        }

        return static_cast<int64_t>(epoch) * 1000000;

    } catch (const std::exception& e) {
        OpenSync::Logger::warn("❗ Failed to parse Oracle timestamp: " + ts + " (" + e.what() + ")");
        return 0;
    }
}*/

/*int64_t parseISOTimestampToMicros(const std::string& value) {
    try {
        size_t dotPos = value.find('.');
        std::string datetimePart = (dotPos != std::string::npos)
            ? value.substr(0, dotPos)
            : value;

        std::string microPart = (dotPos != std::string::npos)
            ? value.substr(dotPos + 1)
            : "0";

        // Parse "YYYY-MM-DD HH:MM:SS" using std::get_time
        std::tm tm = {};
        std::istringstream ss(datetimePart);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail()) {
            return -1;
        }

        // Convert to time_t
        std::time_t timeSec = std::mktime(&tm);
        if (timeSec == -1) {
            return -1;
        }

        // Convert time to microseconds since epoch
        auto tp = std::chrono::system_clock::from_time_t(timeSec);
        int micros = 0;
        try {
            micros = std::stoi(microPart.substr(0, 6));  // truncate to max 6 digits
        } catch (...) {
            micros = 0;
        }

        auto total = std::chrono::duration_cast<std::chrono::microseconds>(
            tp.time_since_epoch()).count();
        return total + micros;
    } catch (...) {
        return -1;
    }
}*/

int64_t parseISOTimestampToMicros(const std::string& value) {
    try {
        // Split value into datetime and microsecond parts
        size_t dotPos = value.find('.');
        std::string datetimePart = (dotPos != std::string::npos)
            ? value.substr(0, dotPos)
            : value;

        std::string microPart = (dotPos != std::string::npos)
            ? value.substr(dotPos + 1)
            : "0";

        // Parse datetime
        std::tm tm = {};
        std::istringstream ss(datetimePart);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail()) {
            return -1;
        }

        tm.tm_isdst = -1;
        std::time_t timeSec = std::mktime(&tm);

        // If mktime fails and year < 1970, fallback manually
        if (timeSec == -1 && tm.tm_year + 1900 < 1970) {
            std::tm epoch_tm = {};
            epoch_tm.tm_year = 70;  // 1970
            epoch_tm.tm_mon = 0;
            epoch_tm.tm_mday = 1;
            epoch_tm.tm_hour = 0;
            epoch_tm.tm_min = 0;
            epoch_tm.tm_sec = 0;

            std::time_t epochSec = std::mktime(&epoch_tm);
            std::time_t adjusted = std::mktime(&tm);

            if (epochSec == -1 || adjusted == -1) return -1;

            int64_t diffSec = static_cast<int64_t>(std::difftime(adjusted, epochSec));
            int micros = 0;
            try {
                micros = std::stoi(microPart.substr(0, 6));
            } catch (...) {}

            int64_t result = diffSec * 1000000LL + micros;

            // ✅ Log timestamp âm
            OpenSync::Logger::debug("🕒 Parsed pre-1970 timestamp: " + value + " → micros = " + std::to_string(result));
            return result;
        }

        // Normal path
        auto tp = std::chrono::system_clock::from_time_t(timeSec);
        int micros = std::stoi(microPart.substr(0, 6));
        int64_t result = std::chrono::duration_cast<std::chrono::microseconds>(
                             tp.time_since_epoch()).count() + micros;

        // ✅ Optional: log if timestamp is negative
        if (result < 0) {
            OpenSync::Logger::debug("🕒 Parsed negative timestamp: " + value + " → micros = " + std::to_string(result));
        }

        return result;

    } catch (...) {
        return -1;
    }
}

} // namespace TimeUtils
