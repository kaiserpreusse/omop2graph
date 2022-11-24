import logging

from graphio.graph import run_query_return_results
from graphio.queries import CypherQuery
from neo4j import Driver
from sqlalchemy import Engine, text
from decimal import Decimal
import logging

from omop2graph.model import SimpleNodeMapping, ConceptMapping

log = logging.getLogger(__name__)


def table_to_node_dict(engine: Engine, table_name: str) -> dict:
    with engine.connect() as conn:
        result = conn.execute(text(f"select * from {table_name}"))
        header = result.keys()
        for row in result:
            cast_row = []
            for x in row:
                if isinstance(x, Decimal):
                    x = float(x)

                cast_row.append(x)
            yield dict(zip(header, cast_row))


def connect_nodes(driver: Driver, left: SimpleNodeMapping, right: SimpleNodeMapping, rel_type: str,
                  database: str = None, direction: str = None, rel_params: dict = None):
    """
    Connect two nodes extracted from tables.

    Left is the 'upstream' node (from table X) and right is the 'downstream' node (from table Y, referencing X with
    the primary key of X. The direction of the relationship in Neo4j can be configured (either 'left_to_right' or
    'right_to_left').

    Example:

    Person
    | person_id | date_of_birth | ... |
    | 1 | 1998 | ... |

    Visit_occurrence
    | visit_occurrence_id | person_id | ... |
    | 1 | 1 | ... |

    :param left:
    :param right:
    :param rel_type:
    :param direction:
    :return:
    """
    if not direction:
        direction = 'left_to_right'
    if direction not in ['left_to_right', 'right_to_left']:
        raise ValueError("direction is either 'left_to_right' or 'right_to_left'")

    q = CypherQuery()
    q.append(f"MATCH (a:{left.node_label}), (b:{right.node_label})")
    q.append(f"WHERE a.{left.primary_key} = b.{left.primary_key}")

    if direction == 'left_to_right':
        q.append(f"MERGE (a)-[:{rel_type}]->(b)")
    elif direction == 'right_to_left':
        q.append(f"MERGE (a)<-[r:{rel_type}]-(b)")

    if rel_params:
        q.append(f"SET r = $rel_params")

    query = q.query()
    log.debug(query)

    if rel_params:
        run_query_return_results(driver, query, database, rel_params=rel_params)

    else:
        run_query_return_results(driver, query, database)


def map_node_to_concept(driver: Driver, concept_map: ConceptMapping, source_node_label: str, database: str = None):
    q = f"CREATE INDEX IF NOT EXISTS FOR (e:{source_node_label}) ON (e.{concept_map.field});"
    log.debug(q)
    run_query_return_results(driver, q, database=database)

    q = CypherQuery()
    q.append(f"MATCH (e:{source_node_label})")
    q.append("CALL {")
    q.append("WITH e")
    q.append("MATCH (c:Concept)")
    q.append(f"WHERE e.{concept_map.field} = c.concept_id")
    q.append(f"MERGE (e)-[r:HAS_CONCEPT]->(c)")
    if concept_map.rel_props:
        q.append(f"SET r = $rel_props")
    q.append(f"SET r.source_field = $source_field, r.target_field = $target_field")
    q.append(f"SET e.{concept_map.property_key} = c.concept_name")
    q.append("} IN TRANSACTIONS;")
    query = q.query()
    log.debug(query)

    run_query_return_results(driver, query, database=database, source_field=concept_map.field,
                             target_field=concept_map.property_key, rel_props=concept_map.rel_props)
