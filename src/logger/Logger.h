#pragma once
#include "string"
#include "fstream"
#include "mutex"

enum LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARNING = 2,
    ERROR = 3,
    FATAL = 4
};

class Logger {
public:
    static Logger& getInstance();

    static void setLogLevel(LogLevel level);
    static LogLevel getLogLevel();
    static bool isDebugEnabled();

    static std::string getCurrentTimestamp();

    static void debug(const std::string& message);
    static void info(const std::string& message);
    static void warn(const std::string& message);
    static void error(const std::string& message);
    static void fatal(const std::string& message);

    void setDebugOrAllFile(const std::string& path);
    void setWarnErrorFatalFile(const std::string& path);

private:
    Logger() = default;
    ~Logger();

    void log(LogLevel level, const std::string& message);
    std::string formatLog(LogLevel level, const std::string& message);
    std::string logLevelToString(LogLevel level);

    static LogLevel currentLevel;
    static std::mutex logMutex;

    std::ofstream debugOrAllFile;
    std::ofstream warnOrFatalFile;
};

