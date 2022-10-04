PARENT_PATH=src/prep/experiment
SCHEMA_NAME_CALL_LEVEL=experiments
SCHEMA_NAME_ROUTING_LEVEL=experiments_routing

if [ ${PROJECT_ENVIRONMENT} ] && [ ${PROJECT_ENVIRONMENT} = "development" ]; then
    SCHEMA_NAME_CALL_LEVEL=dev_experiments
    SCHEMA_NAME_ROUTING_LEVEL=dev_experiments_routing
fi


# Setup experiments schema and tables for the call level.
echo "The schema name is $SCHEMA_NAME_CALL_LEVEL."
echo "Setting up experiments schema and tables for the call level..."
psql -v schema_name=${SCHEMA_NAME_CALL_LEVEL} -f ${PARENT_PATH}/setup_experiments_logging_call_level.sql

echo "Finished."

# Setup experiments schema and tables for the routing level.
echo "The schema name is $SCHEMA_NAME_ROUTING_LEVEL."
echo "Setting up experiments schema and tables for the routing level..."
psql -v schema_name=${SCHEMA_NAME_ROUTING_LEVEL} -f ${PARENT_PATH}/setup_experiments_logging_routing_level.sql

echo "Finished."