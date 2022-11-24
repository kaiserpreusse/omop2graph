import logging
import os
from dataclasses import dataclass
from sqlalchemy import Engine
from graphio import NodeSet
from graphio.graph import run_query_return_results
from neo4j import Driver

from omop2graph.database import table_to_node_dict


def load_concepts(engine: Engine, driver: Driver, database: str = None, schema: str = None):
    concepts = NodeSet(['Concept'], ['concept_id'])
    concepts_table_name = 'concept'
    if schema:
        concepts_table_name = f"{schema}.{concepts_table_name}"

    logging.debug(concepts_table_name)

    concepts.nodes = table_to_node_dict(engine, concepts_table_name)

    concepts.create_index(driver, database=database)
    concepts.merge(driver, database=database)
