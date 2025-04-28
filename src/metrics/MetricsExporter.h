#pragma once

#include <map>
#include <string>
#include <mutex>
#include <unordered_map>
#include <utility>

class MetricsExporter {
public:
    static MetricsExporter& getInstance();

    // Simple counter
    void incrementCounter(const std::string& name);

    // Counter with labels
    void incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels);
    void incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels, int count);

    // Simple gauge
    void setMetric(const std::string& name, double value);

    // Gauge with labels
    void setMetric(const std::string& name, double value, const std::map<std::string, std::string>& labels);
    void setGauge(const std::string& name, double value, const std::map<std::string, std::string>& labels);

    // Legacy labeled gauges (string key like table name)
    void incrementGauge(const std::string& name, const std::string& labelKey);
    void decrementGauge(const std::string& name, const std::string& labelKey);

    //void incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels = {}, int amount = 1);

    std::string exportMetrics();

private:
    MetricsExporter() = default;
    ~MetricsExporter() = default;
    MetricsExporter(const MetricsExporter&) = delete;
    MetricsExporter& operator=(const MetricsExporter&) = delete;

    std::mutex metricsMutex;
    std::mutex gaugeMutex;

    // Simple counters
    std::unordered_map<std::string, int> counters;

    // Simple gauges
    std::unordered_map<std::string, double> metrics;

    // Labeled counters
    std::map<std::pair<std::string, std::map<std::string, std::string>>, int> counters_;

    // Labeled gauges
    std::map<std::pair<std::string, std::map<std::string, std::string>>, double> labeledMetrics;

    // Legacy per-label (e.g., table) gauges
    std::unordered_map<std::string, std::unordered_map<std::string, int>> labeledGauges;

    // General gauge metric map with labels
    std::map<std::pair<std::string, std::map<std::string, std::string>>, double> gaugeMetrics;
};

