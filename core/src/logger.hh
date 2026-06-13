#pragma once

// USAGE
// ------------------------------------------------------
// #include "logger.hh"

// int main() {
//     Logger::set_level(LogLevel::Debug);

//     std::string model = "llama-3.gguf";
//     int gpu_layers = 35;

//     LOG_INFO("Loading model '{}'", model);
//     LOG_DEBUG("GPU layers = {}", gpu_layers);
//     LOG_WARN("VRAM utilization is {}%", 91);
//     LOG_ERROR("Failed to open '{}'", "config.json");
// }
// ------------------------------------------------------

#include <format>
#include <iostream>
#include <mutex>
#include <utility>

enum class LogLevel {
  Debug = 0,
  Info,
  Warn,
  Error
};

class Logger {

public:
  static void set_level(LogLevel level) noexcept {
    level_ = level;
  }

  template<typename... Args>
  static void log(LogLevel level, std::format_string<Args...> fmt, Args&&... args) noexcept {
    if (level < level_) return;

    try {
      std::lock_guard<std::mutex> lock(mutex_);
      std::cout
        << "[" << to_string(level) << "] "
        << std::format(fmt, std::forward<Args>(args)...)
      << "\n";
    } catch (...) { /* Logging should not fail the application */ }
  }

private:
  static constexpr const char* to_string(LogLevel level) noexcept {
    switch (level) {
      case LogLevel::Debug : return "DEBUG";
      case LogLevel::Info  : return "INFO";
      case LogLevel::Warn  : return "WARN";
      case LogLevel::Error : return "ERROR";
    }

    return "UNKOWN";
  }

  inline static LogLevel level_ = LogLevel::Info;
  inline static std::mutex mutex_;
};

#define LOG_DEBUG(...) \
  do { Logger::log(LogLevel::Debug, __VA_ARGS__); } while (0)

#define LOG_INFO(...) \
  do { Logger::log(LogLevel::Info, __VA_ARGS__); } while (0)

#define LOG_WARN(...) \
  do { Logger::log(LogLevel::Warn, __VA_ARGS__); } while (0)

#define LOG_ERROR(...) \
  do { Logger::log(LogLevel::Error, __VA_ARGS__); } while (0)