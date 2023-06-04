# AIRBAG1/2/3/4 -> AIRBAG
# HAZMAT_CD1/2/3/4 -> HAZMAT
# HAZMAT_REL_IND1/2/3/4 -> HAZMAT_RELEASE_IND
# HAZMAT_IND?
# VEHICLE MAKE CODES? -> VEHICLE

import json

lookup_dict = json.load("./column_codes.json")


def lookup_column_code(column_name: str, value: str) -> str:
    if column_name.startswith('AIRBAG'):
        search_name = 'AIRBAG'
    elif column_name.startswith('HAZMAT_CD'):
        search_name = 'HAZMAT'
    elif column_name.startswith('HAZMAT_REL_IND'):
        search_name = 'HAZMAT_RELEASE_IND'
    else:
        search_name = column_name

    if search_name in lookup_dict:
        column_dict = lookup_dict[search_name]
        if value in column_dict:
            return column_dict[value]
        else:
            print("No matching value for", value, "for", column_name)
            return None
    else:
        print("No match for column for code lookup for", column_name)
        return None
