"""conftest for pytest"""

import os
import logging
from datetime import datetime

import pandas as pd

import pytest_html
import pytest

# needed file in the directory  __init__.py
from .utils import get_df_test_index
from .utils import write_test_results_to_excel
from .utils import safe_df_result
from .utils import get_dict_by_path


def pytest_html_results_table_header(cells):
    """pytest_html_results_table_header"""

    # <th class="sortable" data-column-type="testId" style="text-align:
    # left; white-space: nowrap" width = "50%">
    for i, cell in enumerate(cells):
        if "testId" in cell:
            # add width to testId cause it is too short in jupyter
            cell = cell.replace('"testId">', '"testId" width = "50%">')
            cells[i] = cell

    # we want to add it only once
    # for example !pytest  -k "01" without directory
    if not any("description" in s for s in cells):
        cells.insert(
            1, '<th class="sortable description" data-column-type="description">Description</th>')
        cells.insert(
            3, '<th class="sortable rowcount" data-column-type="rowcount">Row count</th>')
        cells.insert(
            4, '<th class="sortable queryid" data-column-type="queryid">Query id</th>')


def pytest_html_results_table_row(report, cells):
    """pytest_html_results_table_row"""

    # we want to add it only once
    if not any("description" in s for s in cells):
        description = getattr(report, "description", "")
        rowcount = getattr(report, "rowcount", "")
        queryid = getattr(report, "queryid", "")

        cells.insert(1, f'<td class="col-description">{description}</td>')
        cells.insert(3, f'<td class="col-rowcount">{rowcount}</td>')
        cells.insert(4, f'<td class="col-queryid">{queryid}</td>')


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item):
    """create report element"""
    outcome = yield
    report = outcome.get_result()

    extra = getattr(report, "extra", [])

    if "request" in item.funcargs.keys():
        request = item.funcargs["request"]

        if request.config.pluginmanager.hasplugin('html'):
            htmlpath = request.config.getoption('htmlpath')

            if htmlpath:

                if report.when == "call":

                    logging.debug("item %s %s", str(item), str(item.stash))

                    if "result" in item.stash:

                        df_result = item.stash.get("result", None)

                        if isinstance(df_result, pd.DataFrame):
                            logging.debug("df_result.attrs.keys %s",
                                          str(df_result.attrs.keys()))

                            logging.debug("df_result.attrs %s",
                                          str(df_result.attrs))

                            report.rowcount = str(
                                df_result.attrs.get("rowcount", ""))
                            report.queryid = str(
                                df_result.attrs.get("query_id", ""))

                            if df_result.attrs.get("description"):
                                report.description = str(
                                    df_result.attrs.get("description"))

                            if df_result.attrs.get("diff_summary_list"):
                                logging.debug("diff_summary_list %s", str(
                                    df_result.attrs.get("diff_summary_list")))

                                df_summary = pd.DataFrame(
                                    df_result.attrs.get("diff_summary_list"))

                                df_summary = df_summary.style.format(
                                    thousands=" ", decimal=",", precision=2)

                                df_summary_html = df_summary.to_html(index=False,
                                                                     index_names=False,
                                                                     border=1,
                                                                     na_rep='NA')
                                extra.append(pytest_html.extras.html(
                                    f"<span style='color:black'>{df_summary_html}</span>"))

                                # extra line
                                extra.append(
                                    pytest_html.extras.html("<p></p>"))

                            if df_result.attrs.get("diff_index_list_sample"):

                                logging.debug("diff_index_list_sample %s", str(
                                    df_result.attrs.get("diff_index_list_sample")))

                                df_diff = df_result.filter(
                                    items=df_result.attrs.get(
                                        "diff_index_list_sample"),
                                    axis="index")

                                df_diff = df_diff.style.format(
                                    thousands=" ", decimal=",", precision=2)

                                extra.append(
                                    pytest_html.extras.html(
                                        "<span style='color:black'>"
                                        + f"{df_diff.to_html(index=False, border=1, na_rep='NA')}"
                                        + "</span>"))

                                # extra line
                                extra.append(
                                    pytest_html.extras.html("<p></p>"))

        report.extras = extra

        logging.debug("item.fspath %s", str(item.fspath))
        logging.debug("item.name %s", str(item.name))
        logging.debug("item.config %s", str(item.config))
        logging.debug("item.nodeid %s", str(item.nodeid))


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session):
    """session finish - save xlsx file"""

    if session.config.pluginmanager.hasplugin('html'):
        htmlpath = session.config.getoption('htmlpath')

    output_xlsx = None

    # if html output
    # and we do now write to xlsx
    if htmlpath and not session.config.stash.get("output_xlsx", None):

        report_name = os.path.basename(htmlpath)
        report_name = report_name.replace('.html', '')

        report_dir = os.path.dirname(htmlpath)

        # on top
        href_output_xlsx = report_name + ".xlsx"

        output_xlsx = os.path.join(report_dir, href_output_xlsx)

    if output_xlsx and not os.path.isfile(output_xlsx):

        logging.info("xlsx: %s", output_xlsx)

        logging.debug("xlsx: %s, isfile: %s", output_xlsx,
                      str(os.path.isfile(output_xlsx)))

        logging.debug("session.items: %s", str(session.items))

        test_results_dict = dict()
        for ii in session.items:

            session_item: pytest.Item = ii
            logging.debug("name: %s %s", str(session_item.name),
                          str(type(session_item.stash.get("result", None))))
            logging.debug("nodeid: %s", str(session_item.nodeid))

            if isinstance(session_item.stash.get("result", None), pd.DataFrame):

                df_result: pd.DataFrame = session_item.stash.get(
                    "result", None)

                logging.debug("df_result size %s %s", str(
                    session_item.name), str(df_result.size))

                test_results_dict[session_item.nodeid] = df_result

                logging.debug("yml debug %s", get_dict_by_path(
                    df_result.attrs, '/debug'))

                if get_dict_by_path(df_result.attrs, '/debug'):
                    safe_df_result(session_item.name, report_dir, df_result)

                    logging.info("df_result put into the %s", str(report_dir))

                logging.debug("yml debug DONE %s", get_dict_by_path(
                    df_result.attrs, '/debug'))

            else:

                df_result = pd.DataFrame()
                df_result.attrs["error_msg"] = "No results in stash"
                test_results_dict[session_item.nodeid] = df_result
                logging.debug("item nodeid %s No results in stash",
                              str(session_item.nodeid))

        logging.debug("xlsx: %s, isfile: %s", output_xlsx,
                      str(os.path.isfile(output_xlsx)))

        logging.debug("test_results_dict: %s", str(test_results_dict.keys()))

        if test_results_dict and not os.path.isfile(output_xlsx):

            df_index = get_df_test_index(test_results_dict)
            write_test_results_to_excel(
                df_index, test_results_dict, output_xlsx)

            logging.info("XLSX file: %s", output_xlsx)

            session.config.stash["output_xlsx"] = output_xlsx
            session.config.stash["href_output_xlsx"] = href_output_xlsx


@pytest.hookimpl(trylast=True)
def pytest_html_results_summary(postfix, session: pytest.Session):
    """pytest_html_results_summary"""

    if session.config.stash.get("href_output_xlsx", None):

        href_output_xlsx = session.config.stash.get("href_output_xlsx", None)

        file_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        postfix.extend(["<p><b>Summary Excel</b></p>"])
        postfix.extend(["<p>" + str(file_ts) + "</p>"])

        postfix.extend(
            ['<p><a href="./' + href_output_xlsx + '">' + href_output_xlsx + '</a></p>'])

        # to run it only once
        del session.config.stash["href_output_xlsx"]
