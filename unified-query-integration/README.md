# Unified Query Integration with Spark

This module bridges Apache Spark with the Unified Query API, enabling Piped Processing Language (PPL) queries to be translated by the unified query planner and executed using the Spark SQL engine.

## Overview

The purpose of this module is to enable query interoperability across systems like Spark by:

- Adapting Spark table and schema metadata into Calcite-compatible structures
- Leveraging the Unified Query API to parse and plan PPL queries into logical plans consumable by Spark