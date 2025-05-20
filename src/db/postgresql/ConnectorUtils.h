// src/utils/ConnectorUtils.h
#pragma once
#include "PostgreSQLConnector.h"
#include "../../reader/ConfigLoader.h"

std::unique_ptr<PostgreSQLConnector> createPostgreSQLConnector(const ConfigLoader& config);

