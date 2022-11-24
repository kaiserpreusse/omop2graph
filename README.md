# OMOP to Graph Database
Load data from OMOP CDM into a Neo4j graph database. Works with Neo4j >= 4.4.

> :warning: This is a dev snapshot of a very early version. Things will change.


## Configuration

.env file:

```shell
NEO4J_URI=neo4j://somehost.com
NEO4J_PASSWORD=...
NEO4J_DATABASE=omop
NEO4J_USER=neo4j
POSTGRES_HOST=somepostgres.com
POSTGRES_PORT=5432
POSTGRES_USER=postgres user
POSTGRES_PASSWORD=...
POSTGRES_DATABASE=some database with OMOP schema
POSTGRES_SCHEMA=the OMOP CDM schema
```

## OMOP CDM
![OMOP CDM 5.4 Schema](docs/img/cdm54.png)
Schema of OMOP CDM v5.4.

Documentation: https://ohdsi.github.io/CommonDataModel/index.html

