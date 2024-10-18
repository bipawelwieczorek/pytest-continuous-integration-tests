
"""Utils for pytest"""

import csv
from datetime import datetime
import io
import os
import re
import logging
import pathlib
import glob
import warnings
import pandas as pd

import tomlkit
import yaml

from snowflake.connector.constants import CONNECTIONS_FILE
from snowflake.sqlalchemy import URL


def get_basename_from_testname(name):
    """base name from test name"""
    basename = name
    s = re.search(r"(\[)(.*[.](sql|yml))(\])", basename)
    if s:
        basename = s.group(2)
        basename = basename.replace('.sql', '')
        basename = basename.replace('.yml', '')

    basename = os.path.basename(basename)
    basename = basename.replace(']', '')
    basename = basename.replace(':', '')

    return basename


def get_uniq_sheet_name(item_test_name, sheet_list):
    """create uniq sheet_name"""

    sheet_name = item_test_name

    sheet_name = get_basename_from_testname(sheet_name)

    sheet_name = sheet_name[:31]

    next_number = 1
    while sheet_name in sheet_list or next_number > 100:
        next_number_len = 31 - len(str(next_number)) - 1  # and 1 for dot
        sheet_name = sheet_name[:next_number_len] + "." + str(next_number)
        next_number = next_number + 1

    logging.debug("sheet_name: %s %s", sheet_name, str(len(sheet_name)))

    return sheet_name


def get_df_test_index(test_results):
    """get dataframe index from test_results"""

    index_data = []

    for key, val in test_results.items():
        try:

            logging.debug("key: %s", key)
            df_result: pd.DataFrame = val

            test_name = df_result.attrs.get(
                "test_name") or get_basename_from_testname(key)

            sql_statement = ""

            if isinstance(df_result, pd.DataFrame) and df_result.attrs:
                sql_statement = df_result.attrs.get("sql", "")

                condition = "Passed" if df_result.attrs.get(
                    "condition") else "Failed"
                error_msg = df_result.attrs.get("error_msg")

                sql_desc = str(df_result.attrs.get("description"))
                sql_desc = sql_desc.replace("\n", chr(10))

                if isinstance(sql_statement, str):
                    sql_statement = sql_statement.replace('\n\n', '\n')
                    sql_statement = sql_statement.replace('\r', '')
                    sql_statement = sql_statement.replace('\t', '    ')

                logging.debug("condition: %s error_msg: %s test: %s",
                              condition, error_msg, key)

                index_data.append({"Test name": test_name, "SQL description": sql_desc,
                                   "Diff result": condition, "Error message": error_msg,
                                   "SQL statement": sql_statement})
            else:
                logging.debug("no attrs for %s", key)
                index_data.append({"Test name": test_name, "SQL description": "",
                                   "Diff result": "Passed", "Error message": "",
                                   "SQL statement": ""})

        except Exception as e:
            logging.error("error %s", str(e))
            raise

    return pd.DataFrame(index_data)


def df_to_export(df_result: pd.DataFrame):
    """prepare df to export

    Returns:
        pd.DataFrame: _description_
    """

    # rename columns to uniq names - duplicates
    df_result.columns = pd.io.common.dedup_names(
        df_result.columns, is_potential_multiindex=False)

    df_result = df_result.assign(
        # ValueError: Excel does not support datetimes with timezones.
        # Please ensure that datetimes are timezone unaware before writing to Excel.
        **{
            col: df_result[col].dt.tz_localize(None)
            for col in df_result.columns
            if hasattr(df_result[col], "dt")
        }
    )

    return df_result


def write_test_results_to_excel(index_df, test_results: dict, output_xlsx):
    """Write test resutl to excel file"""

    with pd.ExcelWriter(output_xlsx,
                        engine_kwargs={'options': {'remove_timezone': True}}) as writer:

        # create index

        df = index_df

        sheet_name = "index"
        df.to_excel(writer, sheet_name=sheet_name, index=False,
                    freeze_panes=(1, 0), header=True, engine='openpyxl')

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        (max_row, max_col) = index_df.shape

        # Set the autofilter.
        worksheet.autofilter(0, 0, max_row, max_col - 1)

        # cell_normal_format = workbook.add_format()

        cell_red_format = workbook.add_format()

        cell_red_format.set_bg_color('#FFC7CE')
        cell_red_format.set_font_color('#ED2839')  # red pantone

        cell_green_format = workbook.add_format()
        cell_green_format.set_bg_color('#C6EFCE')  # pastel green
        cell_green_format.set_font_color('#006100')

        #########################################
        # Columns formating
        #########################################
        for i, col in enumerate(df.columns):

            column_len = df[col].astype(str).str.len().max()
            logging.debug("column_len %s: %s", col, str(column_len))

            # Setting the length if the column header is larger
            # than the max column value length
            column_len = max(column_len, len(col) + 3)

            if column_len > 50:
                column_len = 50

            cell_format = workbook.add_format()
            if col in ['Test name', 'Error message', 'SQL description']:
                cell_format.set_text_wrap()

            worksheet.set_column(i, i, column_len, cell_format)

        #########################################
        # Rows formating
        #########################################
        for idx in df.index:

            sql_desc_lines = df['SQL description'][idx].count("\n")

            cell_format = workbook.add_format()
            cell_height = None
            if sql_desc_lines > 1:
                cell_height = 15 * sql_desc_lines
                cell_format.set_text_wrap()
            else:
                # cause if you set 15 it is not working
                cell_height = 15.1

                cell_format.set_align("top")

            worksheet.set_row(idx + 1, cell_height, cell_format)

            if df['Diff result'][idx] == 'Failed':
                worksheet.write(
                    idx + 1, 2, df['Diff result'][idx], cell_red_format)

            if df['Diff result'][idx] == 'Passed':
                worksheet.write(
                    idx + 1, 2, df['Diff result'][idx], cell_green_format)

        # for item in test_results:
        for key, val in test_results.items():

            try:
                df_result: pd.DataFrame = val

                test_name = df_result.attrs.get("test_name") or key

                logging.debug("test_results[%s].keys() %s",  test_name, str(
                    df_result.attrs.keys()))

                logging.debug("condition %s, item %s", str(
                    df_result.attrs.get("condition")), test_name)

                logging.debug("writer.sheets.keys %s",
                              str(writer.sheets.keys()))

                sheet_name = get_uniq_sheet_name(
                    item_test_name=test_name, sheet_list=writer.sheets.keys())

                df_result = df_to_export(df_result)

                df_result.to_excel(writer, sheet_name=sheet_name, index=False, freeze_panes=(
                    1, 0), header=True, engine='openpyxl')

                workbook = writer.book
                worksheet = writer.sheets[sheet_name]

                if not df_result.attrs.get("condition"):
                    worksheet.set_tab_color('red')
                    logging.info("item mark red %s", test_name)

                if not df_result.empty:

                    # Get the dimensions of the dataframe.
                    (max_row, max_col) = df_result.shape

                    # Set the autofilter.
                    worksheet.autofilter(0, 0, max_row, max_col - 1)
                    # worksheet.set_row(0, 40)  # Set the height of Row 1 to 20.

                    dd = df_result.dtypes.to_dict()
                    logging.debug("dtypes %s", str(dd))
                    # Iterate through each column and set the width == the max
                    # length in that column.
                    # A padding length of 2 is also added.
                    for i, col in enumerate(df_result.columns):

                        column_len = df_result[col].astype(str).str.len().max()
                        logging.debug("column_len %s : %s",
                                      col, str(column_len))

                        # Setting the length if the column header is larger
                        # than the max column value length
                        column_len = max(column_len, len(col) + 3)

                        if column_len > 20:
                            column_len = 20

                        logging.debug(
                            "column_len final: %s : %s", col, str(column_len))

                        worksheet.set_column(i, i, column_len)

                        # if END

                # Add a header format.

                diff_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': False,
                    'valign': 'top',
                    'fg_color': '#ED2839',  # red pantone
                    'border': 2})

                diff_columns_names_list = df_result.attrs.get(
                    "diff_col_names_list", dict())
                diff_columns_iloc_list = df_result.attrs.get(
                    "diff_col_iloc_list", dict())

                # colorize values
                diff_colorize_column_indexes = df_result.attrs.get(
                    "diff_colorize_column_indexes", {})
                logging.debug("diff_colorize_column_indexes %s",
                              str(diff_colorize_column_indexes))

                # Write the column headers with the defined format.
                for col_no, col_name in zip(diff_columns_iloc_list, diff_columns_names_list):

                    # colorize header
                    if col_name in diff_columns_names_list:
                        logging.debug("diff_columns [RED] %s %s", str(
                            col_no), str(col_name))
                        worksheet.write(0, col_no, col_name, diff_format)

                        # colorize values

                        if col_name in diff_colorize_column_indexes:
                            diff_column_indexes = diff_colorize_column_indexes.get(
                                col_name, [])
                            logging.debug("diff_column_dict_index %s %s", str(
                                col_name), str(diff_column_indexes))

                            for index_no in diff_column_indexes:
                                if index_no < df_result.shape[0]:
                                    df_val = df_result.iat[index_no, col_no]
                                    diff_light_red = workbook.add_format(
                                        {'bg_color': '#FF7F7F'})  # light red
                                    worksheet.write(
                                        index_no + 1, col_no, df_val, diff_light_red)

            except Exception as e:
                logging.error("error %s", str(e))
                logging.error("sheet_name %s", str(sheet_name))

                raise


def get_dict_from_connection_name(connection_name):
    """Get dict from toml file"""

    warnings.filterwarnings(
        action="ignore",
        message=".*Bad owner or permissions.*",
    )

    try:

        logging.debug("Connection toml file: %s", str(CONNECTIONS_FILE))

        with open(CONNECTIONS_FILE, 'r', encoding="UTF8") as f:
            content = f.read()

        doc = tomlkit.parse(content)[connection_name]

        return doc

    except Exception as inst:

        logging.error("CONNECTIONS_FILE error %s", str(inst))


def get_url_from_connection_name(connection_name):
    """get url from connection name"""

    # **kwargs allows for any number of optional keyword arguments (parameters),
    # which will be in a dict named kwargs.
    return URL(**get_dict_from_connection_name(connection_name))


def get_files(pattern, file):
    """get file list based on pattern"""

    def get_cwd():
        return os.getcwd()

    if os.path.isfile(file):
        file_dir = os.path.dirname(file)
    else:
        file_dir = os.path.abspath(file)

    if isinstance(pattern, list):
        files = []
        for pattern_item in pattern:
            glob_pattern = os.path.join(file_dir, pattern_item)
            files.extend(glob.glob(glob_pattern, recursive=False))

    else:
        glob_pattern = os.path.join(file_dir, pattern)
        files = glob.glob(glob_pattern, recursive=False)

    files = sorted(files, key=os.path.getmtime)

    logging.debug("pattern : " + str(pattern) +
                  ", root_path : " + file_dir + ", glob " + glob_pattern)

    res = []

    for filename in files:

        skip_file = False

        if pathlib.Path(filename).suffix == '.sql' and filename.replace('.sql', '.yml') in files:
            skip_file = True
            logging.debug("yml file : %s sql file is skipped", str(filename))
            logging.debug("sql filename : %s", str(filename))

        if (pathlib.Path(filename).suffix == '.yml' and
                os.path.dirname(filename) == os.path.basename(filename)):
            skip_file = True
            logging.info(
                "default yml file : %s sql file is skipped", str(filename))

        if not skip_file:
            relfile = os.path.relpath(os.path.join(
                file_dir, filename), start=get_cwd())

            logging.debug("relfile : %s", relfile)
            res.append(relfile)

    return res


def get_dict_by_path(input_dict, dict_path, default=None):
    """get dict element based on path e.g. /a/b/c/d"""

    logging.debug(input_dict)

    d = input_dict

    path_keys = dict_path.split('/')

    logging.debug("path_keys: %s", str(path_keys))

    # remove empty
    path_keys = list(filter(None, path_keys))

    key_found = False
    for key in path_keys:

        logging.debug("key: %s", key)
        logging.debug("dict d: %s", str(d))

        key_found = False

        # element is a dict
        if isinstance(d, dict):
            if key in d.keys():
                d = d.get(key)
                key_found = True

        # element is a list, cannot have duplicated keys in the list
        elif isinstance(d, list):
            for item in d:
                logging.debug("list: %s  %s", str(type(item)), str(item))
                if isinstance(item, dict):
                    print("item: " + str(type(item)) + " " + str(item))
                    if key in item.keys():
                        logging.debug("item: %s get: %s", str(
                            key), str(item.get(key)))
                        d = item.get(key)
                        key_found = True

        logging.debug("selected dict d: %s", str(d))

        if not key_found:
            logging.debug("key not found: %s", key)

    if key_found:
        return d
    else:
        return default


def safe_df_result(test_name, output_dir, df_result: pd.DataFrame):
    """save df result to json and save df attrs to yml"""

    logging.info("output_dir: %s", output_dir)

    sql_file = get_dict_by_path(df_result.attrs, 'config-file')

    if not sql_file:
        sql_file = get_dict_by_path(df_result.attrs, 'sql-file')

    if not sql_file:
        sql_file = get_basename_from_testname(test_name)
        logging.info("sql_file: %s %s", sql_file, test_name)

    if sql_file:

        sql_file = sql_file.replace('.sql', '')
        sql_file = os.path.basename(sql_file)
        logging.info("sql_file: %s", sql_file)

        output_file = os.path.basename(sql_file)
        output_file = output_file.replace('.yml', '')
        output_file = output_file + "_" + datetime.now().strftime("%Y%m%d_%H%M")
        output_full_path_file = os.path.join(output_dir, output_file)

        try:

            df_result = df_to_export(df_result)

            logging.info("df type: %s", str(type(df_result)))

            df_result.to_csv(output_full_path_file + ".csv", encoding='utf8', decimal='.',
                             sep=',', date_format='yyyy-mm-dd', quoting=csv.QUOTE_NONNUMERIC)
            logging.info("File: %s", output_file + ".csv")

            df_result.to_json(output_full_path_file +
                              ".json", orient="table", indent=2)
            logging.info("File: %s", output_file + ".json")

        except Exception as e:

            logging.error(str(e))

        df_result.attrs['data-file'] = output_file + ".json"

        try:

            buf = io.StringIO()
            yaml.dump(df_result.attrs, buf,
                      allow_unicode=True, canonical=False)

            content = buf.getvalue()
            content = content.replace(
                '!!python/object/apply:datetime.timedelta', '')
            content = content.replace(
                '!!python/object/apply:numpy.core.multiarray.scalar', '')
            content = content.replace('!!python/object/apply:numpy.dtype', '')

            with open(output_full_path_file + ".yml", mode='w', encoding="UTF8") as f:
                f.writelines(content)

            logging.info("File: %s", output_file + ".yml")

        except Exception as e:

            logging.error(str(e))

    else:
        logging.info("There is no sql_file: %s %s",
                     output_dir, str(df_result.attrs.keys()))


def df_to_native_types(df: pd.DataFrame):
    """convert to proper types

    Returns:
        pd.DataFrame: _description_
    """

    # wrong convertin for commas and dots
    df_conv = df.convert_dtypes(convert_floating=False)

    logging.debug("df.dtypes %s\n", str(df_conv.dtypes))

    for i, col in enumerate(df_conv.select_dtypes(include=['object']).columns):
        try:

            # this is not working cause it tries to cast also header df_conv.iloc[:, i]
            # column do not cast header
            df_conv[col] = df_conv[col].astype(float, errors='ignore')

            logging.debug("object %s %s", str(i), col)
            logging.info("object -> to_numeric: %s %s .. %s", col,
                         str(df_conv[col].min()), str(df_conv[col].max()))

        except Exception as e:
            logging.debug("cannot convert object %s", str(e))
            logging.info("cannot convert object -> to_numeric: %s %s .. %s",
                         col, str(df_conv[col].min()), str(df_conv[col].max()))

    logging.debug("df.dtypes after %s\n", str(df_conv.dtypes))

    return df_conv


def df_info(df):
    """datafreame info"""

    std_output = io.StringIO()
    df.info(buf=std_output)
    return std_output.getvalue()
