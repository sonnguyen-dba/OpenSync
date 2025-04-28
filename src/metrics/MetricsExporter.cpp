#include "MetricsExporter.h"
#include <sstream>
#include <unordered_set>

MetricsExporter& MetricsExporter::getInstance() {
    static MetricsExporter instance;
    return instance;
}

void MetricsExporter::incrementCounter(const std::string& name) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    counters[name]++;
}

void MetricsExporter::incrementCounter(const std::string& name, const std::map<std::string, std::string>& labels) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    counters_[std::make_pair(name, labels)]++;
}

void MetricsExporter::incrementCounter(const std::string& name,
    const std::map<std::string, std::string>& labels, int count) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    counters_[std::make_pair(name, labels)] += count;
}

void MetricsExporter::setMetric(const std::string& name, double value) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    metrics[name] = value;
}

void MetricsExporter::setMetric(const std::string& name, double value,
    const std::map<std::string, std::string>& labels) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    labeledMetrics[std::make_pair(name, labels)] = value;
}

void MetricsExporter::setGauge(const std::string& name, double value,
    const std::map<std::string, std::string>& labels) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    gaugeMetrics[std::make_pair(name, labels)] = value;
}

void MetricsExporter::incrementGauge(const std::string& name,
    const std::string& labelKey) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    labeledGauges[name][labelKey]++;
}

void MetricsExporter::decrementGauge(const std::string& name,
    const std::string& labelKey) {
    std::lock_guard<std::mutex> lock(metricsMutex);
    if (--labeledGauges[name][labelKey] <= 0) {
        labeledGauges[name].erase(labelKey);
    }
}

std::string MetricsExporter::exportMetrics() {
    std::lock_guard<std::mutex> lock(metricsMutex);
    std::ostringstream output;

    std::unordered_set<std::string> helpExported;

    // Simple Gauges
    for (const auto& [key, value] : metrics) {
        if (helpExported.insert(key).second) {
            output << "# HELP " << key << " Prometheus metric\n";
            output << "# TYPE " << key << " gauge\n";
        }
        output << key << " " << value << "\n";
    }

    // Simple Counters
    for (const auto& [key, value] : counters) {
        if (helpExported.insert(key).second) {
            output << "# HELP " << key << " Prometheus counter\n";
            output << "# TYPE " << key << " counter\n";
        }
        output << key << " " << value << "\n";
    }

    // Labeled Counters
    for (const auto& [entry, value] : counters_) {
        const auto& [metric, labels] = entry;
        if (helpExported.insert(metric).second) {
            output << "# HELP " << metric << " Prometheus counter\n";
            output << "# TYPE " << metric << " counter\n";
        }
        output << metric;
        if (!labels.empty()) {
            output << "{";
            bool first = true;
            for (const auto& [k, v] : labels) {
                if (!first) output << ",";
                output << k << "=\"" << v << "\"";
                first = false;
            }
            output << "}";
        }
        output << " " << value << "\n";
    }

    // Labeled Gauges
    for (const auto& [entry, value] : labeledMetrics) {
        const auto& [metric, labels] = entry;
        if (helpExported.insert(metric).second) {
            output << "# HELP " << metric << " Prometheus metric\n";
            output << "# TYPE " << metric << " gauge\n";
        }
        output << metric;
        if (!labels.empty()) {
            output << "{";
            bool first = true;
            for (const auto& [k, v] : labels) {
                if (!first) output << ",";
                output << k << "=\"" << v << "\"";
                first = false;
            }
            output << "}";
        }
        output << " " << value << "\n";
    }

    // Named Labeled Gauges (legacy kiểu map<table,value>)
    for (const auto& [metric, labels] : labeledGauges) {
        if (helpExported.insert(metric).second) {
            output << "# HELP " << metric << " Prometheus gauge with table label\n";
            output << "# TYPE " << metric << " gauge\n";
        }
        for (const auto& [label, val] : labels) {
            output << metric << "{table=\"" << label << "\"} " << val << "\n";
        }
    }

    // GaugeMetrics kiểu mới (map<labels, double>)
    for (const auto& [entry, value] : gaugeMetrics) {
        const auto& [metric, labels] = entry;
        if (helpExported.insert(metric).second) {
            output << "# HELP " << metric << " Prometheus gauge\n";
            output << "# TYPE " << metric << " gauge\n";
        }
        output << metric;
        if (!labels.empty()) {
            output << "{";
            bool first = true;
            for (const auto& [k, v] : labels) {
                if (!first) output << ",";
                output << k << "=\"" << v << "\"";
                first = false;
            }
            output << "}";
        }
        output << " " << value << "\n";
    }

    return output.str();
}

