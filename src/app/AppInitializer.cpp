#include "AppInitializer.h"
#include "../config/FilterConfigLoader.h"
#include "../schema/OracleSchemaCache.h"
#include "../schema/PostgreSQLSchemaCache.h"
#include "../logger/Logger.h"
#include "../sqlbuilder/OracleSQLBuilder.h"
#include "../sqlbuilder/PostgreSQLSQLBuilder.h"
#include "../monitor/MonitorManager.h"
#include "../db/oracle/OracleConnector.h"
#include "../db/postgresql/PostgreSQLConnector.h"

std::unique_ptr<AppComponents> AppInitializer::initialize(const std::string& configPath) {
    auto components = std::make_unique<AppComponents>();

    // Load config
    components->config = std::make_unique<ConfigLoader>(configPath);
    if (!components->config->loadConfig()) {
        OpenSync::Logger::error("❌ Failed to load config: " + configPath);
        return nullptr;
    }

    // Setup logger
    int logLevel = components->config->getInt("log-level", 0);
    OpenSync::Logger::getInstance().setLogLevel(static_cast<OpenSync::LogLevel>(logLevel));
    OpenSync::Logger::getInstance().setDebugOrAllFile("log/syslog.log");
    OpenSync::Logger::getInstance().setWarnErrorFatalFile("log/warn_error_fatal.log");

    if (OpenSync::Logger::isDebugEnabled() || components->config->shouldLogConfigToConsole()) {
        components->config->dumpConfig("config_dump.log", true);
    }

    // Determine DB type
    std::string dbType = components->config->getConfig("db_type", "oracle");
    OpenSync::Logger::info("🔍 Active DB Type: " + dbType);

    // Filter config (patch: truyền dbType để xử lý lowercase nếu PostgreSQL)
    std::string filterConfigPath = components->config->getConfig("filter_config_path", "config/filter_config.json");
    if (!FilterConfigLoader::getInstance().loadConfig(filterConfigPath, dbType)) {
    	OpenSync::Logger::error("❌ Failed to load filter config.");
    	return nullptr;
     }

    // Preload schema and auto refresh
    int interval = components->config->getInt("schema_refresh_secs", 10);
    if (dbType == "oracle" && components->config->getBool("enable-oracle", true)) {
        OracleSchemaCache::getInstance().preloadAllSchemas(*components->config);
        OracleSchemaCache::getInstance().startAutoRefreshThread(*components->config, interval);
    } else if (dbType == "postgresql" && components->config->getBool("enable-postgresql", true)) {
        PostgreSQLSchemaCache::getInstance().preloadAllSchemas(*components->config);
        PostgreSQLSchemaCache::getInstance().startAutoRefreshThread(*components->config, interval);

      /*  std::thread([] {
            while (true) {
                PostgreSQLSchemaCache::getInstance().shrinkIfInactive(300);
                std::this_thread::sleep_for(std::chrono::minutes(10));
            }
        }).detach();*/
    }
    /*else if (dbType == "postgresql" && components->config->getBool("enable-postgresql", true)) {
    	PostgreSQLSchemaCache::getInstance().preloadAllSchemas(*components->config);
    	PostgreSQLSchemaCache::getInstance().startAutoRefreshThread(*components->config, interval);

        // 💡 Force preload for each table from filter
    	for (const auto& f : FilterConfigLoader::getInstance().getAllFilters()) {
            std::string fullTable = f.owner + "." + f.table;
            PostgreSQLSchemaCache::getInstance().loadSchemaIfNeeded(fullTable, *components->config);
        }

        // Background shrink thread
        std::thread([] {
             while (true) {
                 PostgreSQLSchemaCache::getInstance().shrinkIfInactive(300);
                 std::this_thread::sleep_for(std::chrono::minutes(10));
             }
        }).detach();
    }*/


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
    components->processor->setActiveDbType(dbType);

    // Register SQLBuilder
    if (dbType == "oracle") {
        auto sqlBuilder = std::make_unique<OracleSQLBuilder>(
            *components->config,
            components->processor->enableISODebugLog
        );
        components->processor->registerSQLBuilder("oracle", std::move(sqlBuilder));
    } else if (dbType == "postgresql") {
        auto sqlBuilder = std::make_unique<PostgreSQLSQLBuilder>(
            *components->config,
            components->processor->enableISODebugLog
        );
        components->processor->registerSQLBuilder("postgresql", std::move(sqlBuilder));
    }

    // Kafka Consumer
    components->consumer = std::make_unique<KafkaConsumer>(
        *components->processor,
        components->config->getKafkaConfig("bootstrap.servers"),
        components->config->getKafkaConfig("topic"),
        components->config->getKafkaConfig("group.id"),
        components->config->getKafkaConfig("auto.offset.reset"),
	components->config->getKafkaConfig("enable.auto.commit"),
        *components->metrics,
        filterConfigPath
    );

    // Start auto-reload for filter config
    components->processor->startAutoReload(filterConfigPath, components->consumer.get());

    // WriteDataToDB and register DBConnector factory
    components->writeData = std::make_unique<WriteDataToDB>();
    if (dbType == "oracle") {
        components->writeData->addDatabaseConnectorFactory("oracle", [config = components->config.get()]() {
            return std::make_unique<OracleConnector>(
                config->getDBConfig("oracle", "host"),
                std::stoi(config->getDBConfig("oracle", "port")),
                config->getDBConfig("oracle", "user"),
                config->getDBConfig("oracle", "password"),
                config->getDBConfig("oracle", "service")
            );
        });
    } else if (dbType == "postgresql") {
        components->writeData->addDatabaseConnectorFactory("postgresql", [config = components->config.get()]() {
            return std::make_unique<PostgreSQLConnector>(
                config->getDBConfig("postgresql", "host"),
                std::stoi(config->getDBConfig("postgresql", "port")),
                config->getDBConfig("postgresql", "user"),
                config->getDBConfig("postgresql", "password"),
                config->getDBConfig("postgresql", "dbname")
            );
        });
    }

    // Start Monitor
    MonitorManager::startMonitors(
        *components->consumer,
        *components->writeData,
        *components->config
    );

    OpenSync::Logger::info("✅ App initialized successfully.");
    return components;
}

