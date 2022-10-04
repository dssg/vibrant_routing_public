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
echo "Creating ${FOLDER_PATH} folder..."
mkdir ${FOLDER_PATH}


# Create log folder
read -p "You are about to generate a blank log folder. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating log folder..."
    mkdir ${FOLDER_PATH}/log/
    # touch ${FOLDER_PATH}/log/test.txt
fi

# Create matrices folder
read -p "You are about to generate a blank matrices folder. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating matrices folder..."
    mkdir ${FOLDER_PATH}/matrices/
    # touch ${FOLDER_PATH}/matrices/test.txt
fi

# Create models folder
read -p "You are about to generate a blank models folder. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating models folder..."
    mkdir ${FOLDER_PATH}/models/
    # touch ${FOLDER_PATH}/models/test.txt
fi

# Create plots folder
read -p "You are about to generate a blank plots folder. Are you sure? [n = no | y = yes]" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating plots folder..."
    mkdir ${FOLDER_PATH}/plots/
    # touch ${FOLDER_PATH}/plots/test.txt
fi