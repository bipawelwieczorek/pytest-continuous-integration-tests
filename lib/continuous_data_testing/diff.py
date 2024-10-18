
"""Module providing a function for difference dataframe data."""

import logging
from datetime import datetime

import pandas as pd

from .utils import get_dict_by_path


def apply_diff_by_column_name(df: pd.DataFrame, colorize=True):
    """
    apply_diff columns with name DIFF != 0

    Added attribute to df
    df.attrs["diff_col_names_list"] 
    df.attrs["diff_col_iloc_list"] 
    df.attrs["diff_index_list"] 
    """

    # data-test:
    #     diff_by_column_name:
    #       limit: 0.1

    diff_limit = get_dict_by_path(
        df.attrs, '/data-test/diff_by_column_name/limit')
    diff_colorize = get_dict_by_path(
        df.attrs, '/data-test/diff_by_column_name/colorize')

    logging.info("YML colorize: %s", str(get_dict_by_path(
        df.attrs, '/data-test/diff_by_column_name/colorize')))

    if not diff_colorize:
        diff_colorize = colorize
        logging.info("colorize : %s", str(colorize))

    if diff_limit and str(diff_limit).isnumeric():
        diff_limit_int = int(diff_limit)
    else:
        diff_limit_int = 0.0

    logging.info("diff_limit: %s %s", diff_limit, str(diff_limit_int))

    t1_start = datetime.now()

    diff_cols = [(i, col) for i, col in enumerate(
        df.columns) if "diff" in col.casefold()]

    diff_col_names_list = list()
    diff_col_iloc_list = list()
    diff_index_list_sample = list()
    diff_colorize_column_indexes = {}
    diff_summary_list = list()

    for i, col in diff_cols:

        df_col = df.iloc[:, i]

        df_diff = pd.DataFrame()

        # if column is a string
        if isinstance(df_col.dtype, pd.StringDtype):

            logging.info("Column type is string: %s %s", col, df_col.dtype)
            df_diff = df.where(df_col.notnull()).where(df_col != '').where(
                df_col != '0').dropna(how='all', axis='index')

        else:

            logging.debug("Column type is : %s %s", col, df_col.dtype)

            logging.debug("%s abs", col)
            df_diff = df.where(df_col.notnull()).where(
                df_col.abs() > diff_limit_int).dropna(how='all', axis='index')
            logging.debug("%s diff_limit != 0", col)

        if not df_diff.empty:

            diff_col_names_list.append(col)
            diff_col_iloc_list.append(i)
            logging.debug("Difference found, column: %s, type: %s, count: %s",
                          col, df_col.dtype, df_col.count())

            diff_index_list_sample.extend(df_diff.head(5).index.to_list())

            # if we want to coloreze Excel
            if diff_colorize:
                logging.info("Colorize column %s", col)
                diff_colorize_column_indexes[col] = list(
                    set(df_diff.index.to_list()))

            if df_diff.shape[0] > 0:
                logging.debug("%s Diff records perc %s %s %s",
                              df_col.name, str(
                                  round(100*df_diff.shape[0]/df.shape[0], 2)),
                              str(df_diff.shape[0]), str(df.shape[0]))

                diff_summary_list.append({"column name": df_col.name,
                                          "diff min": df_diff[df_col.name].min(),
                                          "diff max": df_diff[df_col.name].max(),
                                          "diff records": df_diff.shape[0],
                                          "total records": df.shape[0],
                                          "diff [%]": round(100*df_diff.shape[0]/df.shape[0], 2)})
                logging.info("%s done", df_col.name)

    # create uniq index list
    diff_index_list_sample = list(set(diff_index_list_sample))

    logging.debug("diff_col_names_list     %s", str(diff_col_names_list))
    logging.debug("diff_col_iloc_list      %s", str(diff_col_iloc_list))
    logging.debug("diff_colorize_column_indexes      %s",
                  str(diff_colorize_column_indexes))
    logging.debug("diff_index_list type    %s", str(diff_index_list_sample))
    logging.debug("diff_summary_list type  %s", str(diff_summary_list))

    df.attrs["diff_col_names_list"] = diff_col_names_list
    df.attrs["diff_col_iloc_list"] = diff_col_iloc_list
    df.attrs["diff_colorize_column_indexes"] = diff_colorize_column_indexes

    df.attrs["diff_index_list_sample"] = diff_index_list_sample
    df.attrs["diff_summary_list"] = diff_summary_list

    # there were no erros
    if "error_msg" not in df.attrs.keys():
        df.attrs["condition"] = len(diff_col_names_list) == 0
        df.attrs["error_msg"] = "!!! Values > " + str(diff_limit_int) + " for columns: " + \
            ", ".join(diff_col_names_list) if diff_col_names_list else None

    t2_finish = datetime.now()

    logging.info('Diff time: %s', (t2_finish - t1_start))
