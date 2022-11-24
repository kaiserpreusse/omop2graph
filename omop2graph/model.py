from typing import List
from dataclasses import dataclass


@dataclass
class ConceptMapping:
    """
    Mapping defintion of a foreign key to the Concept table.

    E.g. The field 'visit_concept_id' is a foreign key to the Concept table.
    The node should be linked to the corresponding concept and the 'concept_name' should
    be stored on the source node in a new property.
    """
    field: str
    property_key: str
    rel_props: dict = None


@dataclass
class SimpleNodeMapping:
    table_name: str  # name of the source table
    primary_key: str  # primary key of the source table
    node_label: str  # target node label in Neo4j
    concept_fields: List[ConceptMapping] = None  # list of mappings for fields referencing Concept table
    column_prefix: str = None  # prefix used for _concept_id, _source_value, ... fields
    date_reference_field: str = None  # field used to extract the primary date


# metadata
Person = SimpleNodeMapping('person', 'person_id', 'Person',
                           concept_fields=[ConceptMapping('gender_concept_id', 'gender')])

VisitOccurrence = SimpleNodeMapping('visit_occurrence', 'visit_occurrence_id', 'Visit',
                                    concept_fields=[ConceptMapping('visit_concept_id', 'visit_type')])

VisitDetail = SimpleNodeMapping('visit_detail', 'visit_detail_id', 'VisitDetail')
ObservationPeriod = SimpleNodeMapping('observation_period', 'observation_period_id', 'ObservationPeriod')

# events
ConditionOccurrence = SimpleNodeMapping('condition_occurrence', 'condition_occurrence_id', 'Condition',
                                        column_prefix='condition',
                                        date_reference_field='condition_start_date', concept_fields=[
        ConceptMapping('condition_status_concept_id', 'condition_status')])

Observation = SimpleNodeMapping('observation', 'observation_id', 'Observation', column_prefix='observation',
                                date_reference_field='observation_date')

DrugExposure = SimpleNodeMapping('drug_exposure', 'drug_exposure_id', 'Drug', column_prefix='drug',
                                 date_reference_field='drug_exposure_start_date')

ProcedureOccurrence = SimpleNodeMapping('procedure_occurrence', 'procedure_occurrence_id', 'Procedure',
                                        column_prefix='procedure',
                                        date_reference_field='procedure_date')

Measurement = SimpleNodeMapping('measurement', 'measurement_id', 'Measurement', column_prefix='measurement',
                                date_reference_field='measurement_date')

DeviceExposure = SimpleNodeMapping('device_exposure', 'device_exposure_id', 'Device', column_prefix='device',
                                   date_reference_field='device_exposure_start_date')

# container objects for secondary labels
# NOTE: the main concept ID for events is linked to concepts via the Event node, not individual nodes
Event = SimpleNodeMapping('_', '_event_id', 'Event',
                          concept_fields=[ConceptMapping('_event_concept_id', 'name')])

# node groups
PERSON_VISIT_NODE_MAPPING = [Person, VisitOccurrence, VisitDetail, ObservationPeriod]
EVENT_NODE_MAPPING = [ConditionOccurrence, Observation, DrugExposure, ProcedureOccurrence, Measurement, DeviceExposure]


def table_name(table_name: str, schema: str = None) -> str:
    """
    Input table name and schema name and return schema.table_name if schema is not empyt.
    Used to normalize between Postgres and MySQL.

    :param table_name:
    :param schema:
    :return:
    """
    if not table_name:
        raise ValueError("Empty table name")
    if schema:
        table_name = f"{schema}.{table_name}"
    return table_name
