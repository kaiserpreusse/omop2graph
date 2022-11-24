import logging

from graphio import NodeSet
from neo4j import Driver
from sqlalchemy import Engine

from omop2graph.database import table_to_node_dict, connect_nodes, map_node_to_concept
from omop2graph.model import table_name, PERSON_VISIT_NODE_MAPPING, Person, VisitDetail, VisitOccurrence

log = logging.getLogger(__name__)


def load_metadata(engine: Engine, driver: Driver, database: str = None, schema: str = None):

    for map in PERSON_VISIT_NODE_MAPPING:
        nodeset = NodeSet([map.node_label], [map.primary_key])
        nodeset.nodes = table_to_node_dict(engine, table_name(map.table_name, schema))
        nodeset.create_index(driver, database=database)
        nodeset.merge(driver, database=database)

    connect_nodes(driver, VisitOccurrence, VisitDetail, 'HAS_DETAIL', database=database, direction='left_to_right')
    connect_nodes(driver, Person, VisitOccurrence, 'HAS_VISIT', database=database, direction='left_to_right')


def name_metadata(driver: Driver, database: str = None):
    for map in PERSON_VISIT_NODE_MAPPING:
        if map.concept_fields:
            for concept_mapping in map.concept_fields:
                map_node_to_concept(driver, concept_mapping, map.node_label, database=database)

