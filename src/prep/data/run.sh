PARENT_PATH=src/prep/data
DB_CONFIG_FILE=config/data_to_database_config.yaml
PRE_COMPUTED_FEATURES_FILE=config/pre_computed_features_config.yaml


# Upload raw data into the database.
read -p "You are about to upload raw data into the database. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Uploading raw data into the database..."
    python ${PARENT_PATH}/data_to_database.py --config ${DB_CONFIG_FILE}
fi

# Create routing id mapping table.
read -p "You are about to create routing id mapping table. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating routing id mapping table..."
    psql -f ${PARENT_PATH}/create_routing_id_mapping.sql
fi


# Set up processed schema and tables.
read -p "You are about to set up processed schema and tables. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Setting up processed schema and tables..."
    psql -f ${PARENT_PATH}/raw_to_processed.sql
fi


# Create pre_computed_features schema and table.
read -p "You are about to create pre_computed_features schema and table. This will take ~6 hours. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    read -p "Are you REALLY sure you want to RUN pre_computed_features? This will take ~SIX hours. Are you sure? [n = no | y = yes]" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        echo "Creating pre-computed features..."
        python ${PARENT_PATH}/pre_computed_features_creator.py --config ${PRE_COMPUTED_FEATURES_FILE}
    fi
fi


echo "Finished."