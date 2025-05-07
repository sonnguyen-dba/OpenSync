#include "Logger.h"
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>

namespace OpenSync {
  LogLevel Logger::currentLevel = INFO;
  std::mutex Logger::logMutex;

  Logger& Logger::getInstance() {
      static Logger instance;
      return instance;
  }

  void Logger::setLogLevel(LogLevel level) {
      currentLevel = level;
  }

  LogLevel Logger::getLogLevel() {
      return currentLevel;
  }

  bool Logger::isDebugEnabled() {
      return currentLevel <= DEBUG;
  }

  void Logger::setDebugOrAllFile(const std::string& path) {
      debugOrAllFile.open(path, std::ios::app);
  }

  void Logger::setWarnErrorFatalFile(const std::string& path) {
      warnOrFatalFile.open(path, std::ios::app);
  }

  void Logger::debug(const std::string& msg) {
      if (currentLevel <= DEBUG) getInstance().log(DEBUG, msg);
  }
  void Logger::info(const std::string& msg) {
      if (currentLevel <= INFO) getInstance().log(INFO, msg);
  }
  void Logger::warn(const std::string& msg) {
      if (currentLevel <= WARNING) getInstance().log(WARNING, msg);
  }
  void Logger::error(const std::string& msg) {
      if (currentLevel <= ERROR) getInstance().log(ERROR, msg);
  }
  void Logger::fatal(const std::string& msg) {
      if (currentLevel <= FATAL) getInstance().log(FATAL, msg);
  }

  std::string Logger::formatLog(LogLevel level, const std::string& message) {
      auto now = std::chrono::system_clock::now();
      auto time = std::chrono::system_clock::to_time_t(now);
      auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

      std::ostringstream oss;
      oss << "[" << std::put_time(std::localtime(&time), "%F %T")
          << "." << std::setfill('0') << std::setw(3) << millis.count() << "] "
          << "[" << logLevelToString(level) << "] "
          << message;
      return oss.str();
  }

  std::string Logger::logLevelToString(LogLevel level) {
      switch (level) {
          case INFO: return "🟢 INFO";
          case WARNING: return "🟠 WARN";
          case DEBUG: return "🐞 DEBUG";
          case ERROR: return "🔴 ERROR";
          case FATAL: return "💥 FATAL";
          default: return "UNKNOWN";
      }
  }

  void Logger::log(LogLevel level, const std::string& message) {
      std::string formatted = formatLog(level, message);
      std::lock_guard<std::mutex> lock(logMutex);

      std::ostream& out = (level >= WARNING) ? std::cerr : std::cout;
      out << formatted << std::endl;

      if (level <= DEBUG && debugOrAllFile.is_open()) {
          debugOrAllFile << formatted << std::endl;
      }
      if (level >= WARNING && warnOrFatalFile.is_open()) {
          warnOrFatalFile << formatted << std::endl;
      }
  }

  Logger::~Logger() {
      if (debugOrAllFile.is_open()) debugOrAllFile.close();
      if (warnOrFatalFile.is_open()) warnOrFatalFile.close();
  }

  std::string Logger::getCurrentTimestamp() {
      auto now = std::chrono::system_clock::now();
      auto t = std::chrono::system_clock::to_time_t(now);
      std::tm tm{};
  #if defined(_MSC_VER)
      localtime_s(&tm, &t);
  #else
      localtime_r(&t, &tm);
  #endif
      std::ostringstream oss;
      oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
      return oss.str();
  }
}
