import os
from typing import Dict, Any, Optional

import yaml

from .table_schemas import ColumnDefinition, BaseEntitySchema, TableSchema
from src.migration.strategies.user_strategies import (
    create_user_events_strategy,
    create_users_targets_strategy,
    create_user_events_smart_strategy,
    create_users_targets_smart_strategy,
)
from src.migration.strategies.quiz_strategies import (
    create_quizzs_links_questions_strategy,
    create_users_quizzs_links_questions_strategy,
)
from src.migration.strategies.content_strategies import (
    create_users_contents_reads_strategy,
)
from src.migration.strategies.coaching_strategies import (
    create_days_contents_links_strategy,
    create_days_logbooks_links_strategy,
    create_coaching_reasons_strategy,
    create_coaching_reasons_smart_strategy,
)

DEFAULT_SCHEMA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "config", "schemas.yaml")
)

# Strategy Factories: Maps strategy names to factory functions
# Use "smart" versions for optimal performance (50-100x faster for typical incremental changes)
STRATEGY_FACTORIES = {
    # Smart strategies (recommended) - use diff-based optimization
    "user_events": create_user_events_smart_strategy,
    "users_targets": create_users_targets_smart_strategy,
    "coaching_reasons": create_coaching_reasons_smart_strategy,

    # Legacy strategies (fallback) - full delete-and-insert pattern
    "user_events_legacy": create_user_events_strategy,
    "users_targets_legacy": create_users_targets_strategy,
    "coaching_reasons_legacy": create_coaching_reasons_strategy,

    # Other relationship strategies (TODO: migrate to SmartDiffStrategy)
    "quizzs_links_questions": create_quizzs_links_questions_strategy,
    "users_quizzs_links_questions": create_users_quizzs_links_questions_strategy,
    "users_contents_reads": create_users_contents_reads_strategy,
    "days_contents_links": create_days_contents_links_strategy,
    "days_logbooks_links": create_days_logbooks_links_strategy,
}


def _build_column_definitions(columns_config):
    return [ColumnDefinition(**column) for column in columns_config]


def _resolve_strategy(strategy_name: Optional[str]):
    if not strategy_name:
        return None
    factory = STRATEGY_FACTORIES.get(strategy_name)
    if not factory:
        raise ValueError(f"Unknown import strategy: {strategy_name}")
    return factory()


def _load_yaml_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if "tables" in data:
        return data["tables"]
    return data


def load_schemas(schema_path: str = DEFAULT_SCHEMA_PATH) -> Dict[str, TableSchema]:
    tables_config = _load_yaml_schema(schema_path)
    schemas: Dict[str, TableSchema] = {}

    for key, config in tables_config.items():
        include_base = config.get("include_base", False)
        name = config.get("name")
        mongo_collection = config.get("mongo_collection")
        export_order = config.get("export_order", 0)
        strategy = _resolve_strategy(config.get("import_strategy"))
        force_reimport = config.get("force_reimport", False)
        truncate_before_import = config.get("truncate_before_import", False)

        if include_base:
            additional_columns = _build_column_definitions(config.get("additional_columns", []))
            additional_mappings = config.get("additional_mappings", {})
            schema = BaseEntitySchema.create_with_base(
                additional_columns=additional_columns,
                name=name or key,
                mongo_collection=mongo_collection,
                additional_mappings=additional_mappings,
                export_order=export_order,
                import_strategy=strategy,
                force_reimport=force_reimport,
                truncate_before_import=truncate_before_import,
            )
        else:
            columns = _build_column_definitions(config.get("columns", []))
            schema = TableSchema.create(
                columns=columns,
                name=name or key,
                mongo_collection=mongo_collection,
                explicit_mappings=config.get("explicit_mappings"),
                export_order=export_order,
                import_strategy=strategy,
                unique_constraints=config.get("unique_constraints"),
                force_reimport=force_reimport,
                truncate_before_import=truncate_before_import,
            )

        if schema.name is None:
            schema.name = key
        if schema.mongo_collection is None:
            schema.mongo_collection = key

        schemas[key] = schema

    return schemas


TABLE_SCHEMAS = load_schemas()
