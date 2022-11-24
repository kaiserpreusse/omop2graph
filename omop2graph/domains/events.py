import logging
from sqlalchemy import Engine
from graphio import NodeSet
from graphio.graph import run_query_return_results
from neo4j import Driver

from omop2graph.database import table_to_node_dict, connect_nodes, map_node_to_concept
from omop2graph.model import table_name, EVENT_NODE_MAPPING
from omop2graph.model import table_name, PERSON_VISIT_NODE_MAPPING, Person, VisitDetail, VisitOccurrence, Event

log = logging.getLogger(__name__)


def load_events(engine: Engine, driver: Driver, database: str = None, schema: str = None):
    for map in EVENT_NODE_MAPPING:
        nodeset = NodeSet(['Event', map.node_label], [map.primary_key])
        nodeset.nodes = table_to_node_dict(engine, table_name(map.table_name, schema))
        nodeset.create_index(driver, database=database)
        nodeset.merge(driver, database=database)


def create_standard_fields(driver: Driver, database: str = None):
    """
    condition_occurrence_id -> _event_concept_id
    observation_id -> _event_concept_id

    condition_occurrence_start_date -> _event_date
    observation_date -> _event_date

    :param driver:
    :param database:
    :return:
    """
    log.info("Set standard fields _event_concept_id, _event_data")
    for map in EVENT_NODE_MAPPING:
        q = f"""MATCH (e:{map.node_label}) SET e._event_concept_id = e.{map.column_prefix}_concept_id SET e._event_date = e.{map.date_reference_field}"""
        log.debug(q)
        run_query_return_results(driver, q, database=database)


def link_events_to_metadata(driver: Driver, database: str = None):
    """
    Link Event -> Visit -> Person

    :param driver:
    :param database:
    :return:
    """
    connect_nodes(driver, VisitOccurrence, Event, 'HAS_EVENT', database=database, direction='left_to_right')
    connect_nodes(driver, VisitDetail, Event, 'HAS_EVENT', database=database, direction='left_to_right')


def chain_events(driver: Driver, database: str = None):
    # NEXT relationships within type
    for map in EVENT_NODE_MAPPING:
        q = f"""MATCH (p:Person)-[:HAS_VISIT]->(:Visit)-[:HAS_EVENT]->(e:{map.node_label})
    WITH p, e ORDER BY e.{map.column_prefix}_start_date
    WITH p, collect(e) AS events
    CALL apoc.nodes.link(events, 'NEXT_SAME_TYPE')
    RETURN 0"""
        log.debug(q)
        run_query_return_results(driver, q, database=database)

    # NEXT between all
    q = """MATCH (p:Person)-[:HAS_VISIT]->(:Visit)-[:HAS_EVENT]->(e:Event)
    WITH p, e ORDER BY e._event_date
    WITH p, collect(e) AS events
    CALL apoc.nodes.link(events, 'NEXT')
    RETURN 0"""
    log.debug(q)
    run_query_return_results(driver, q, database=database)


def name_events(driver: Driver, database: str = None):
    for map in EVENT_NODE_MAPPING:
        if map.concept_fields:
            for concept_mapping in map.concept_fields:
                map_node_to_concept(driver, concept_mapping, map.node_label, database=database)

    # map Event container
    for concept_mapping in Event.concept_fields:
        map_node_to_concept(driver, concept_mapping, Event.node_label, database=database)
