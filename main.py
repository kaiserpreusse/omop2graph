import os
from sqlalchemy import create_engine
from neo4j import GraphDatabase
import logging

from omop2graph.domains.events import load_events, create_standard_fields, link_events_to_metadata, chain_events, name_events
from omop2graph.domains.person import load_metadata, name_metadata

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
logging.getLogger('neo4j').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", None)
NEO4J_USER = os.getenv("NEO4J_USER", None)
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", None)
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", None)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", None)
POSTGRES_PORT = os.getenv("POSTGRES_PORT", None)
POSTGRES_USER = os.getenv("POSTGRES_USER", None)
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", None)
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", None)
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", None)

log.debug([NEO4J_URI, NEO4J_USER, NEO4J_DATABASE])
log.debug([POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_DATABASE, POSTGRES_SCHEMA])


engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}",
                       echo=True)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


#load_concepts(engine, driver, database=NEO4J_DATABASE, schema='cdm')
load_metadata(engine, driver, database=NEO4J_DATABASE, schema='cdm')
name_metadata(driver, database=NEO4J_DATABASE)

load_events(engine, driver, database=NEO4J_DATABASE, schema='cdm')
create_standard_fields(driver, database=NEO4J_DATABASE)
link_events_to_metadata(driver, database=NEO4J_DATABASE)
chain_events(driver, database=NEO4J_DATABASE)
name_events(driver, database=NEO4J_DATABASE)
