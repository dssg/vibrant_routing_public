from hashlib import md5

import yaml
import json
from config.project_constants import MODELING_CONFIG_FILE


def fill_na(data, column_name, imputation_strategy=None):
    """Fill null data in column with defined imputation strategy.

    Keyword arguments:
        data (pd.DataFrame) -- Data to be imputed.
        column_name (str) --  Name of column to be imputed.
        imputation_strategy (str, optional) -- Imputation strategy to use. Defaults to NoneType.

    Returns:
        data (pd.DataFrame) -- data with all null values imputed according to the `imputation_strategy`.
    """
    if imputation_strategy == "min":
        # Replace none value with the minimum data from data[column].
        value = data.loc[:, column_name].min()
        data.loc[:, column_name] = data.loc[:, column_name].fillna(value)
    elif imputation_strategy == "mean":
        # Replace none value with the average data from data[column].
        value = data.loc[:, column_name].mean()
        data.loc[:, column_name] = data.loc[:, column_name].fillna(value)
    elif imputation_strategy == "max":
        # Replace none value with the maximum data from data[column].
        value = data.loc[:, column_name].max()
        data.loc[:, column_name] = data.loc[:, column_name].fillna(value)
    else:
        # Replace none value with 1/len(data).
        value = 1 / len(data)
        data.loc[:, column_name] = data.loc[:, column_name].fillna(value)
    return data


def create_hash(dict_to_hash):
    """Calculate md5sum hash from to_hash object.

    Keyword arguments:
        dict_to_hash (dict) -- dictionary with the values that need to be joined
                               in one single string to later be hashed.
    """
    str_to_hash = "\n".join(dict_to_hash.values())
    return md5(str(str_to_hash).encode("utf-8")).hexdigest()


def get_feature_groups_dict(feature_config):
    """Get the feature groups and feature names from the config file.

    Keyword argument:
        feature_config (dict) -- dictionary with the parameters configuration for features.

    Returns:
        feature_groups_dict(dict) -- dictionary with feature groups as keys
                                     and the feature names as values.
    """
    feature_config_keys = ["query_fillings", "query_fillings_augment"]
    feature_groups_dict = {}

    for feature_config_key in feature_config_keys:
        query_fillings = feature_config[feature_config_key]

        for feature_group, feature_group_param in query_fillings.items():
            query_filling = feature_group_param["query_filling"]
            parameter_1 = feature_group_param.get("parameter_1", [])
            parameter_2 = feature_group_param.get("parameter_2", [])
            parameter_3 = feature_group_param.get("parameter_3", [])

            feature_groups_dict[feature_group] = []
            feature_name = query_filling.split("as")[-1].strip().strip('"')

            if len(parameter_3) != 0:
                feature_names = [
                    f"{feature_name}".format(
                        parameter_1=p1, parameter_2=p2, parameter_3=p3
                    )
                    for p1 in parameter_1
                    for p2 in parameter_2
                    for p3 in parameter_3
                ]
            elif len(parameter_2) != 0:
                feature_names = [
                    f"{feature_name}".format(parameter_1=p1, parameter_2=p2)
                    for p1 in parameter_1
                    for p2 in parameter_2
                ]
            elif len(parameter_1) != 0:
                feature_names = [
                    f"{feature_name}".format(parameter_1=p1) for p1 in parameter_1
                ]
            else:
                feature_names = [feature_names]

            feature_groups_dict[feature_group].extend(feature_names)

    return feature_groups_dict


def main():
    """Main function to exemplify how to use the function."""
    # Read yaml file containing database configuration for modeling
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    result = get_feature_groups_dict(feature_config=modeling_config["feature_config"])
    print(json.dumps(result, indent=4))


# main()
