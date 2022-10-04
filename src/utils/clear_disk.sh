DEV=""
if [ ${PROJECT_ENVIRONMENT} ] && [ ${PROJECT_ENVIRONMENT} = "development" ]; then
    DEV="dev_"
fi

# Set the parent path
PARENT_PATH=$(pwd)
read -p "This is the current parent path: ${PARENT_PATH}. Do you confirm that this is the correct PARENT_PATH? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Nn]$ ]]
then
    read -p "Please introduce the value of interest for PARENT_PATH: "
    echo
    PARENT_PATH=$REPLY
fi

# Set the folder path
FOLDER_PATH=${PARENT_PATH}/${DEV}model_governance
read -p "You are about to be asked if you would like to remove the content of ${FOLDER_PATH}. [Press any key to continue]"

# Remove log folder
read -p "You are about to remove the log folder and its content. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing log folder..."
    rm -r ${FOLDER_PATH}/log
fi

# Remove matrices folder
read -p "You are about to remove the matrices folder and its content. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing matrices folder..."
    rm -r ${FOLDER_PATH}/matrices
fi

# Remove models folder
read -p "You are about to remove the models folder and its content. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing models folder..."
    rm -r ${FOLDER_PATH}/models
fi

# Remove plots folder
read -p "You are about to remove the plots folder and its content. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing plots folder..."
    rm -r ${FOLDER_PATH}/plots
fi