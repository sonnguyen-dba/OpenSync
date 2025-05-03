#include "AppInitializer.h"
#include "../config/FilterConfigLoader.h"
#include "../schema/oracle/OracleSchemaCache.h"
#include "../schema/postgresql/PostgreSQLSchemaCache.h"
#include "../logger/Logger.h"
#include "../sqlbuilder/OracleSQLBuilder.h"
#include "../monitor/MonitorManager.h"

std::unique_ptr<AppComponents> AppInitializer::initialize(const std::string& configPath) {
    auto components = std::make_unique<AppComponents>();

    // Load config
    components->config = std::make_unique<ConfigLoader>(configPath);
    if (!components->config->loadConfig()) {
        Logger::error("❌ Failed to load config: " + configPath);
        return nullptr;
    }

    // Setup logger
    int logLevel = components->config->getInt("log-level", 0);
    Logger::getInstance().setLogLevel(static_cast<LogLevel>(logLevel));
    Logger::getInstance().setDebugOrAllFile("log/syslog.log");
    Logger::getInstance().setWarnErrorFatalFile("log/warn_error_fatal.log");

    if (Logger::isDebugEnabled() || components->config->shouldLogConfigToConsole()) {
        components->config->dumpConfig("config_dump.log", true);
    }

    // Filter config
    std::string filterConfigPath = components->config->getConfig("filter_config_path", "config/filter_config.json");
    if (!FilterConfigLoader::getInstance().loadConfig(filterConfigPath)) {
        Logger::error("❌ Failed to load filter config.");
        return nullptr;
    }

    // Preload schema + Auto refresh + GC
    if (components->config->getBool("enable-oracle", true)) {
    	OracleSchemaCache::getInstance().preloadAllSchemas(*components->config);
    	OracleSchemaCache::getInstance().startAutoRefreshThread(*components->config, 10);
    	std::thread([] {
           while (true) {
             OracleSchemaCache::getInstance().shrinkIfInactive(30);
             std::this_thread::sleep_for(std::chrono::minutes(10));
           }
      }).detach();
    }

    // PostgreSQL schema cache: preload + auto-refresh + shrink
    if (components->config->getBool("enable-postgresql", true)) {
        PostgreSQLSchemaCache::getInstance().preloadAllSchemas(*components->config);
        PostgreSQLSchemaCache::getInstance().startAutoRefreshThread(*components->config, 3);
        std::thread([] {
            while (true) {
                PostgreSQLSchemaCache::getInstance().shrinkIfInactive(30);
                std::this_thread::sleep_for(std::chrono::minutes(10));
            }
        }).detach();
    }

    // Metrics server
    components->metrics = std::make_unique<MetricsServer>(
        components->config->getInt("prometheus_port", 8087)
    );

    // Kafka Processor
    components->processor = std::make_unique<KafkaProcessor>(*components->config);
    components->processor->setKafkaTopic(components->config->getKafkaConfig("topic"));
    components->processor->startDedupCleanup();
    for (const auto& f : FilterConfigLoader::getInstance().getAllFilters()) {
        components->processor->addFilter(f);
    }
    components->processor->enableISODebugLog = components->config->getBool("debug_iso_log", false);

    // SQL Builder (Oracle)
    auto sqlBuilder = std::make_unique<OracleSQLBuilder>(
        *components->config,
        components->processor->enableISODebugLog
    );
    components->processor->registerSQLBuilder("oracle", std::move(sqlBuilder));

    // Kafka Consumer
    components->consumer = std::make_unique<KafkaConsumer>(
        *components->processor,
        components->config->getKafkaConfig("bootstrap.servers"),
        components->config->getKafkaConfig("topic"),
        components->config->getKafkaConfig("group.id"),
        components->config->getKafkaConfig("auto.offset.reset"),
        *components->metrics,
        filterConfigPath,
        components->config->getKafkaConfig("enable.auto.commit")
    );

    //👉 Start auto-reload cho cả Processor và Consumer
    components->processor->startAutoReload(filterConfigPath, components->consumer.get());

    // WriteDataToDB
    components->writeData = std::make_unique<WriteDataToDB>();
    components->writeData->addDatabaseConnectorFactory("oracle", [config = components->config.get()]() {
        return std::make_unique<OracleConnector>(
            config->getDBConfig("oracle", "host"),
            std::stoi(config->getDBConfig("oracle", "port")),
            config->getDBConfig("oracle", "user"),
            config->getDBConfig("oracle", "password"),
            config->getDBConfig("oracle", "service")
        );
    });

    // 👉 Start MonitorManager
    MonitorManager::startMonitors(
    	*components->consumer,
    	*components->writeData,
    	*components->config
    );
    Logger::info("✅ App initialized successfully.");
    return components;
}
