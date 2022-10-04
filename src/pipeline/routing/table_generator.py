import pandas as pd
import math
import yaml
import random
import click

from config.project_constants import MODELING_CONFIG_FILE
from config.data_for_table_generator import list_of_states, area_codes_to_state_mapping

# TODO: set up logging


def is_change_allowed(
    npanxx_to_change, old_assignment_package, new_assignment_package, old_routing_table
):
    """Verify whether the change is allowed.

    Keyword arguments:
        npanxx_to_change (integer) -- the row to change.
        old_assignment_package (list[tuple]) -- list of tuples about the previous four centers.
        new_assignment_package (list[tuple]) -- list of tuples about the new four centers.
            Each tuple looks like: (center_key, termination_number, center_role)
            Example: [(NJ973000, 7325326744.0, PrimaryFIPSCountyCode)]
        old_routing_table (pd.DataFrame) -- a routing table.

    Returns:
        is_allowed (bool) -- whether the change is allowed.
    """
    # Print status.
    print(f"NPANXX {npanxx_to_change}: Validating the change...")

    # Assume True to start.
    is_allowed = True

    # Verify that there are exactly four elements because there are four slots in the table.
    if len(new_assignment_package) != 4:
        print(
            "The proposed change is invlaid because there should be exactly four elements in the proposed change."
        )
        is_allowed = False

    # Verify that the proposed new centers actually exist.
    # TODO: need to find a better way to get a list of centers that exist.
    centers_that_exist = pd.unique(
        old_routing_table[
            ["center1id", "center2id", "center3id", "center4id"]
        ].values.ravel()
    )

    for new_assignment_element in new_assignment_package:
        if new_assignment_element is not None:
            new_center_id, _, _ = new_assignment_element
            if new_center_id not in centers_that_exist:
                print(
                    "The proposed change is invalid because one of the center_ids is not real."
                )
                is_allowed = False

    # If your state is the KEY, then you can make calls to VALUE state.
    # TODO: find a better way to get a mapping of state contract agreements.
    # -- they currently don't include the inter-state agreements bc idk what they are.
    contract_allowances = {}
    for s in list_of_states:
        contract_allowances[s] = [s]

    # Area code is the first three digits of the NPANXX.
    area_code_of_npanxx = str(npanxx_to_change)[:3]
    state_of_npanxx = area_codes_to_state_mapping[area_code_of_npanxx]

    # Verify that the states and contracts are real and allowed.
    for new_assignment_element in new_assignment_package:
        if new_assignment_element is not None:
            new_center_id, _, _ = new_assignment_element
            state_of_new_center = str(new_center_id)[:2]
            # Verify that is a real state.
            if state_of_new_center not in list_of_states:
                is_allowed = False
                print(
                    "The proposed change is invalid because the state of one of the new centers is not real."
                )
            # Verify that the within-state contractual agreements is allowed.
            if state_of_new_center not in contract_allowances[state_of_npanxx]:
                is_allowed = False
                print(
                    "The proposed change is invalid because the state of one of the new centers falls outside of in-state contract agreements."
                )

    # Verify what happens if one of the proposed call centers is None.
    for i in range(4):
        if new_assignment_package[i] == None:
            # If it is None, then everything after it should be None too.
            if not all(x is None for x in new_assignment_package[i + 1 :]):
                print(
                    "The proposed change is invalid because one of the new centers was assigned to None, but a subsequent center was not None."
                )
                is_allowed = False

            # Center 1 should never be None.
            if i == 0:
                print(
                    "The proposed change is invalid because the new center 1 cannot be set to None."
                )
                is_allowed = False

    # Verify that there aren't any repeats in the list.
    proposed_new_center_ids = []
    # Get the list of new center ids that are not None.
    for new_assignment_element in new_assignment_package:
        if new_assignment_element is not None:
            new_center_id, _, _ = new_assignment_element
            proposed_new_center_ids.append(new_center_id)
    # If everything in that list is unique, then len(list) == len(set(list)).
    if len(proposed_new_center_ids) != len(set(proposed_new_center_ids)):
        is_allowed = False
        print(
            "The proposed change is invalid because the same center_id appears more than once in the proposed assignment."
        )
        print(len(proposed_new_center_ids), proposed_new_center_ids)
        print(len(set(proposed_new_center_ids)), set(proposed_new_center_ids))

    # TODO: Verify that the id, termination, role match each other.

    return is_allowed


# TODO: consider -- instead of inputting the four proposed the new centers as a list of four elements,
# input just the ones that you want to change?
def change_one_row(routing_table, npanxx_to_change, new_assignment_package):
    """For the given npanxx_to_change, change the call center assignment. Also, validate that the change (new_assignment_package) is allowed.

    Keyword arguments:
        routing_table (pd.Dataframe) -- the old routing table.
        npanxx_to_change (integer) -- the row to change.
        new_assignment_package (list[tuple]) -- list of tuples about the four centers.
            Each tuple looks like: (center_key, termination_number, center_role)
            Example: [(NJ973000, 7325326744.0, PrimaryFIPSCountyCode)]

    Returns:
        routing_table (pd.DataFrame) -- the modified routing table with the implemented change.
    """
    # Get information about npanxx-of-interest's current situation.
    row_to_change = routing_table.loc[npanxx_to_change]
    old_center_ids = [
        row_to_change[ctr]
        for ctr in ["center1id", "center2id", "center3id", "center4id"]
    ]
    old_center_terminations = [
        row_to_change[ctr]
        for ctr in [
            "center1termination",
            "center2termination",
            "center3termination",
            "center4termination",
        ]
    ]
    old_center_roles = [
        row_to_change[ctr]
        for ctr in ["center1role", "center2role", "center3role", "center4role"]
    ]
    old_assignment_package = tuple(
        zip(old_center_ids, old_center_terminations, old_center_roles)
    )

    # Check if the change is allowed.
    is_allowed = is_change_allowed(
        npanxx_to_change=npanxx_to_change,
        old_assignment_package=old_assignment_package,
        new_assignment_package=new_assignment_package,
        old_routing_table=routing_table,
    )

    # If allowed, implement the change.
    if is_allowed:
        # Enumerate starting at 1 because the first call center is called "center1".
        # Loop over the four elements in both lists, and implement the changes one by one.
        for i, (old_assignment_element, new_assignment_element) in enumerate(
            zip(old_assignment_package, new_assignment_package), 1
        ):
            # If the new assignment differs from the old assignment, then implement the changes
            # to 1) center id, 2) center termination, 3) center role.
            if (
                old_assignment_element != new_assignment_element
                and new_assignment_element is not None
            ):
                routing_table.at[
                    npanxx_to_change, f"center{i}id"
                ] = new_assignment_element[0]
                routing_table.at[
                    npanxx_to_change, f"center{i}termination"
                ] = new_assignment_element[1]
                routing_table.at[
                    npanxx_to_change, f"center{i}role"
                ] = new_assignment_element[2]

                # In the columns designated to noting change, make an indication that something changed.
                # TODO: add six more columns -- to specify which column changed?
                routing_table.at[npanxx_to_change, f"ctr_{i}_changed"] = 1
                routing_table.at[npanxx_to_change, "datechanged"] = pd.Timestamp.now()
            # If the new assignment is just the same as the old assignment, then don't do anything.
            else:
                pass

    # Print whether the proposed change to this NPANXX was allowed overall.
    print(
        f"NPANXX {npanxx_to_change}: Was the proposed change to the centers assigned to this exchange code allowed?",
        is_allowed,
    )

    # Print a separator between iterations.
    print("*" * 30)

    # Return back the routing table.
    return routing_table, is_allowed


def generate_table(original_table_filepath, change_dict):
    """From an existing table, change the table by taking a subset of exchange codes and changing the call centers assigned to it.

    Keyword arguments:
        original_table_filepath (str) -- path where to find the routing table of interest.
        change_dict (dict[int:tuple]) -- dictionary about which rows to change.
                              Maps npanxx: (center_key, termination_number, center_role])

    Returns:
        routing_table (pd.DataFrame) -- the new routing table.
    """

    # If change_dict contains NaNs, convert the tuple to Nonetype.
    for npanxx_to_change, new_assignment_package in change_dict.items():
        for i, new_assignment_element in enumerate(new_assignment_package):
            new_center_id, _, _ = new_assignment_element
            if not type(new_center_id) == str and math.isnan(new_center_id):
                new_assignment_package[i] = None
        change_dict[npanxx_to_change] = new_assignment_package

    # Load routing table to pd.DataFrame and set column names to lower case.
    routing_table = pd.read_csv(original_table_filepath, low_memory=False)
    routing_table.columns = routing_table.columns.str.lower()

    # Set the exchange code as index in the routing table to ease upcoming lookups.
    routing_table["npanxx"] = pd.to_numeric(routing_table["npanxx"])
    routing_table = routing_table.set_index("npanxx")

    # List of the columns that are supposed to indicate whether a row was changed from the previous routing table.
    cols_to_indicate_change = [
        "ctr_1_changed",
        "ctr_2_changed",
        "ctr_3_changed",
        "ctr_4_changed",
    ]

    # Zero out said columns. (This line creates those columns if they do not already exist).
    for col_to_indicate_change in cols_to_indicate_change:
        routing_table[col_to_indicate_change] = 0

    # For every desired change listed in change_dict, check if that change is allowed.
    is_anything_allowed = False
    for npanxx_to_change, new_assignment_package in change_dict.items():
        routing_table, is_allowed = change_one_row(
            routing_table=routing_table,
            npanxx_to_change=npanxx_to_change,
            new_assignment_package=new_assignment_package,
        )
        if is_allowed:
            is_anything_allowed = True

    # A new table was created if any of the call center assignments differ from the previous table,
    # i.e. if any of the propsed changes stated in change_dict were allowed.
    if is_anything_allowed:
        return routing_table
    else:
        return None


def generate_change_dict_by_swapping_ctrs_1_2(original_table, row_number_to_change):
    """Generates a change_dict by swapping centers 1 and 2

    Keyword arguments:
        original_table (pd.DataFrame) -- the original table to base the new table off of.
        row_number_to_change (int) -- integer value of the row to be changed.

    Returns:
        change_dict (dict) -- contains the desired changes.
            Maps NPANXX to their new center assignments; each mapped tuple looks like: (center_key, termination_number, center_role)
            Example: {
                    201201 :[(NJ973000, 7325326744.0, PrimaryFIPSCountyCode)],
                }

        save_filename (str) -- where to save the table that will be created based off this change_dict (filename only; no filepath needed)
    """
    row_to_change = original_table.iloc[row_number_to_change]

    npanxx = row_to_change["npanxx"]

    center1id = row_to_change["center1id"]
    center2id = row_to_change["center2id"]
    center3id = row_to_change["center3id"]
    center4id = row_to_change["center4id"]

    center1termination = row_to_change["center1termination"]
    center2termination = row_to_change["center2termination"]
    center3termination = row_to_change["center3termination"]
    center4termination = row_to_change["center4termination"]

    center1role = row_to_change["center1role"]
    center2role = row_to_change["center2role"]
    center3role = row_to_change["center3role"]
    center4role = row_to_change["center4role"]

    change_dict = {
        npanxx: [
            (center2id, center2termination, center2role),
            (center1id, center1termination, center1role),
            (center3id, center3termination, center3role),
            (center4id, center4termination, center4role),
        ]
    }

    save_filename = f"{npanxx}_{center2id}_{center1id}_simulated_routing_table.csv"

    return change_dict, save_filename


@click.command()
@click.option(
    "--num_tables_to_create",
    prompt="Number of tables you want to create",
    default="5",
)
def main(num_tables_to_create):
    """Example function to show the functioning of the table generator.

    Keyword arguments:
        num_tables_to_create (str or int) -- desired number of new tables to be generated and saved.
            Note that some generated tables will fail the validation checks, so it may be the case that
            the actual number of saved new tables is less than this.
            This number is request from user input when the script is run. Defaults to 5.

    Saves to disk:
        routing_table.csv -- the new routing table.
    """
    # Read yaml file containing database configuration for modeling.
    with open(MODELING_CONFIG_FILE) as f:
        modeling_config = yaml.load(f, Loader=yaml.FullLoader)

    simulator_config = modeling_config["routing_level_config"]["simulator_config"]
    original_table_filepath = simulator_config["original_table_filepath"]
    original_table = pd.read_csv(
        original_table_filepath,
        low_memory=False,
    )
    original_table.columns = original_table.columns.str.lower()

    # TODO: error checking -- what if the npanxx or area code isn't real?

    # List of unknown area codes -- to be filled in during the forloop.
    possible_npanxx_with_unknown_area_codes = []

    # Which row of the original routing table to start changing.
    # The row number is chosen by using a random integer generator
    # to randomly choose an integer between 0 and 200.
    start_seed = random.randint(0, 200)
    # The last possible row of the original routing table that can be changed.
    # This number is read in from the simulator_config portion of the modeling_config file.
    end_seed = simulator_config["row_count_original_table"]
    # From user input: the number of tables to create. Note that not every table creation.
    # attempt will be successful, so the user may end up with fewer tables than desired.
    number_of_tables_to_create = int(num_tables_to_create)
    # Generate new change_dicts on every {increment} number of charts.
    increment = int((end_seed - start_seed) / number_of_tables_to_create)
    for i in range(start_seed, end_seed, increment):
        # Generate the new change_dict.
        # Here we choose to generate change_dicts by swapping centers 1 and 2.
        # This is not the only way to do so, but so far it is the only function we have.
        change_dict, save_filename = generate_change_dict_by_swapping_ctrs_1_2(
            original_table=original_table, row_number_to_change=i
        )

        try:
            # Create the new table.
            new_table = generate_table(
                original_table_filepath=original_table_filepath,
                change_dict=change_dict,
            )

            # Save the new routing table to disk.
            if new_table is not None:
                save_filepath = simulator_config["save_filepath"]
                new_table.to_csv(f"{save_filepath}{save_filename}")

        # Assumption: the KeyError is due to unknown area codes.
        except KeyError:
            possible_npanxx_with_unknown_area_codes.append(change_dict.keys())
            continue

    print(
        f"List of POSSIBLY UNKNOWN AREA CODES from this run: {possible_npanxx_with_unknown_area_codes}"
    )


main()
