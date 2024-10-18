"""class SnowflakeTestRunner"""


import os
from datetime import datetime
from contextlib import ContextDecorator
import re
import logging
import http.client as http_client
import importlib_metadata

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import yaml


import pandas as pd

# from .utils import get_dict_by_path
from .utils import get_url_from_connection_name
from .utils import df_to_native_types
from .utils import df_info


class SnowflakeTestRunner(ContextDecorator):
    """Class Snowflake Test"""

    def __init__(self, connection_name=None, metadata=None, env=None):

        self.params: dict = env

        if metadata:
            self.params = self.params | metadata

        if not self.params:
            self.params = {}

        # uppercase all keys
        self.params = {k.upper(): v for k, v in self.params.items()}

        if connection_name:
            self.params['CONNECTION_NAME'] = connection_name

        if 'CONNECTION_NAME' in self.params:
            self.connection_name = self.params['CONNECTION_NAME']

            logging.debug("self.connection_name: %s", self.connection_name)

            connection_url = get_url_from_connection_name(self.connection_name)

            self.engine = create_engine(connection_url)
        else:
            self.connection_name = None
            self.engine = None
            logging.error("There is no connection_name")

        logging.getLogger("snowflake.connector.cursor").setLevel(logging.ERROR)
        logging.getLogger(
            "snowflake.connector.connection").setLevel(logging.ERROR)

        http_client.HTTPConnection.debuglevel = 0

    def __enter__(self):

        logging.debug("with-statement contexts")
        return self

    # with-statement contexts finished
    def __del__(self):
        # self.engine.dispose()
        logging.debug("with-statement contexts")

    # we will turn off the light

    def __exit__(self, *exc):

        if self.engine:
            self.engine.dispose()
            logging.debug(
                "with-statement contexts dispose")

    def get_info(self):
        """instance info"""
        sf_params = ['HOST', 'AUTHENTICATOR', 'ACCOUNT',
                     'USER', 'WAREHOUSE', 'ROLE', 'DATABASE']

        res = []

        for param in sf_params:
            param_val = os.environ.get(param, '<' + param + ">")
            res.append(param + " = " + param_val)

        return res

    def __str__(self):

        return str(self.engine) + str(self.get_info())

    def get_replace_regex_metadata(self, in_txt, metadata_dict: dict):
        """replace string"""

        out_txt = in_txt

        if metadata_dict:
            for key, value in metadata_dict.items():
                logging.debug("%s %s", key, value)
                logging.debug("%s %s", value.get(
                    'pattern', None), value.get('repl', None))

                pattern = value.get('pattern', None)
                repl = value.get('repl', None)
                repl_param = self.params.get(repl.upper(), None)
                logging.debug("%s : %s -> %s %s", key,
                              pattern, repl, repl_param)

                if repl_param:
                    out_txt = re.sub(pattern, repl_param, out_txt)

                    if in_txt != out_txt:
                        logging.info("%s : %s -> %s", key, pattern, repl_param)

        return out_txt

    def get_sql(self, sql_file):
        """get sql from file"""

        logging.debug("sql_file: %s", sql_file)

        sql_statement = open(sql_file, 'r', encoding="utf-8").read()

        return sql_statement

    def get_doc_comment(self, sql_statement):
        """ get /** */ (Javadoc comments) 

            from the sql 

            Javadoc comments are imbedded inside /** ... */ tags
            """

        m = re.match(r"(\/[\*]{2}[^\/]*[\*]\/)", sql_statement, re.MULTILINE)
        if m:
            sql_desc = m.group(0)

            sql_desc = re.sub(
                r"(?smi)\/[\*]{2}\s*[\*]", "", sql_desc, re.MULTILINE)
            sql_desc = re.sub(r"[\*]\/", "", sql_desc)
            sql_desc = re.sub(r"[\n]\s*[\*]", "\n", sql_desc, re.MULTILINE)

            sql_desc = re.sub(r"^\s*", "", sql_desc, re.MULTILINE)
            sql_desc = re.sub(r"[\n]+\s*", "\n", sql_desc, re.MULTILINE)
        else:
            sql_desc = ""

        logging.info("description: %s", sql_desc)

        return sql_desc

    def get_default_yaml_filename(self, yml_file):
        """get default yaml file if exists"""

        yml_dirname = os.path.dirname(yml_file)
        logging.debug("dir name %s ", yml_dirname)

        default_yml_file = os.path.join(os.path.dirname(yml_file),
                                        os.path.dirname(yml_file) + '.yml')

        if os.path.isfile(default_yml_file):
            logging.debug("default yml %s ", default_yml_file)

            return default_yml_file

    def get_yaml_file(self, yml_file):
        """get Yaml configuration """

        sql_dict = dict()

        if not os.path.isfile(yml_file):
            logging.error("File does not exit: %s", yml_file)
            return None

        with open(yml_file, 'r', encoding="utf-8") as f:
            yaml_data = yaml.full_load(f)
            sql_dict = yaml_data

        sql_dict['config-file'] = yml_file

        # if 'sql-file' in sql_dict.keys():
        if 'sql-file' in sql_dict:
            logging.info("sql-file tag found, keys: %s", yaml_data.keys())

            # the same name as the file
            if sql_dict['sql-file'] == r'${self:basename}.sql':

                self_yml_name = os.path.basename(yml_file).replace('.yml', '')
                logging.info("self name %s -> %s",
                             '${self:basename}', self_yml_name)
                sql_dict['sql-file'] = sql_dict['sql-file'].replace(
                    r'${self:basename}', self_yml_name)

            sql_file = os.path.join(os.path.dirname(
                yml_file), sql_dict['sql-file'])
            logging.info("sql-file %s: %s",  sql_dict['sql-file'], sql_file)

            if os.path.isfile(sql_file):
                logging.info("get sql file %s:",  sql_file)
                sql_dict['sql'] = open(sql_file, 'r', encoding="utf-8").read()

        if 'sql' in sql_dict and 'sql-file' not in sql_dict:
            logging.info("sql tag found, keys: %s", str(sql_dict.keys()))

        return sql_dict

    def get_sql_from_params(self):
        """get parameter from metadata WAREHOUSE or SESSION_VARIABLE"""

        session_stmt_list = []
        if 'WAREHOUSE' in self.params.keys():
            session_stmt_list.append(
                f"USE WAREHOUSE {self.params.get('WAREHOUSE', '')};")

        if self.params.get('SESSION_VARIABLE', '').upper().startswith("SET "):
            param_session_variable = "/* param   */ " + \
                self.params.get('SESSION_VARIABLE', '')
            session_stmt_list.append(param_session_variable)

        return session_stmt_list

    def log_df_info(self, df, msg: str):
        """log df info"""

        logging.info("%s", msg)

        if 'log' not in df.attrs:
            df.attrs["log"] = []

        df.attrs["log"].append(msg)

    def run_sql(self, sql_stmt=None, sql_file=None, sql_formatted=None, dry_run=False):
        """run sql"""

        if not self.engine:
            dry_run = True
            logging.error("Dry_run. There is no engine")

        if not dry_run:

            t1_start = datetime.now()

            # not sure if this is working
            self.engine.execution_options(
                stream_results=True, max_row_buffer=10000)
            # not empty dict
            if self.engine.get_execution_options() != {}:
                logging.info("execution_options: %s", str(
                    dict(self.engine.get_execution_options())))
                logging.info("stream_results: %s", self.engine.get_execution_options().get(
                    "stream_results", "None"))

            with self.engine.connect() as conn:

                run_session_list = []

                try:
                    t2_connected = datetime.now()

                    run_sql_stmt = sql_stmt or sql_formatted.get(
                        'sql') or self.get_sql(sql_file)

                    run_session_list.extend(self.get_sql_from_params())

                    if 'session' in sql_formatted:
                        run_session_list.extend(sql_formatted['session'])

                    logging.debug("run_list: %s", str(run_session_list))

                    for run_stmt in run_session_list:
                        conn.execute(text(run_stmt))
                        logging.info("SQL execution: %s", run_stmt)

                    if importlib_metadata.version('pandas') < "2.2.2":

                        # it keeps numpy types
                        # ot working with sqlachemy 2.2
                        df = pd.read_sql_query(text(run_sql_stmt), conn)
                    else:
                        # not sure if steam_result is working or buffer
                        resultset = conn.execution_options(
                            stream_results=True, max_row_buffer=10000).execute(text(run_sql_stmt))

                        df = pd.DataFrame(
                            resultset.all(), columns=resultset.keys())

                    df = df_to_native_types(df)

                    self.log_df_info(df, f"[run_sql] df.info {df_info(df)}")

                    t3_executed = datetime.now()

                    df.attrs["rowcount"] = len(df)

                    df.attrs["query_id"] = conn.execute(
                        "SELECT LAST_QUERY_ID() AS query_id").first()[0]

                    df.attrs["connection_time"] = t2_connected - t1_start
                    df.attrs["query_time"] = t3_executed - t2_connected

                    self.log_df_info(
                        df, f"query_id: {str(df.attrs.get('query_id'))} "
                            + f"rowcount: {str(df.attrs.get('rowcount'))}")
                    self.log_df_info(
                        df, "SELECT * FROM TABLE(RESULT_SCAN"
                            + f"(\'{str(df.attrs.get('query_id'))}\'));")
                    self.log_df_info(
                        df, f"connection time: {(t2_connected - t1_start)},"
                            + f" query time: {(t3_executed - t2_connected)}")

                except SQLAlchemyError as e:

                    if 'df' not in locals():
                        df = pd.DataFrame()

                    logging.error(str(e))
                    self.log_df_info(df, str(e))
                    df.attrs["error_msg"] = str(e)
                    df.attrs["condition"] = False
                    df.attrs["connection_time"] = t2_connected - t1_start

                finally:
                    conn.close()

                    if sql_formatted:
                        df.attrs.update(sql_formatted)

        else:
            df = pd.DataFrame()

        return df

    def run_test(self, config_file, dry_run=False):
        """ run test from yml or sql
        """

        sql_formatted = {}

        if config_file:

            logging.info("config_file: %s", str(config_file))

            # check yml default

            if config_file.endswith(".yml"):
                logging.info("YAML yml file: %s", str(config_file))
                sql_formatted = self.get_yaml_file(config_file)

                # logging.info("YAML yml file: %s", str(config_file))

                yaml_default_file = self.get_default_yaml_filename(config_file)

                if yaml_default_file:
                    sql_formatted_default = self.get_yaml_file(
                        yaml_default_file)

                    if sql_formatted_default:
                        logging.info("YAML default: %s",
                                     str(yaml_default_file))

                        # sql_formatted =  sql_formatted | sql_formatted_default

                        for key, val in sql_formatted_default.items():

                            if not key in sql_formatted:
                                logging.info(
                                    "YAML key %s not found set default:  %s", key, val)
                                sql_formatted[key] = val

            if config_file.endswith(".sql"):

                # get sql from the file
                logging.info("SQL file: %s", str(config_file))
                file_sql_stmt = self.get_sql(config_file)

                logging.info("YAML sql_file: %s", str(config_file))
                yaml_default_file = self.get_default_yaml_filename(config_file)
                logging.info("YAML default config: %s", yaml_default_file)

                if yaml_default_file:
                    sql_formatted = self.get_yaml_file(yaml_default_file)

                    sql_formatted['config-file'] = config_file
                    config_file = None

                sql_formatted['sql'] = file_sql_stmt

            # apply dates replacement
            if 'sql' in sql_formatted and 'metadata' in sql_formatted:
                sql_formatted['sql'] = self.get_replace_regex_metadata(
                    sql_formatted.get('sql', None), sql_formatted.get('metadata', None))
            else:
                logging.info("No YML metadata tag found")

            logging.debug("description: %s", str(
                sql_formatted.get('description', '')))

            if sql_formatted.get('description', '') == '':
                # get description from sql
                logging.debug("no description in the sql: %s",
                              str(sql_formatted.get('sql', '')))
                sql_formatted['description'] = self.get_doc_comment(
                    sql_formatted.get('sql', ''))

            logging.debug("[run_sql_file] sql %s: ",
                          str(sql_formatted.get('sql')))
            logging.debug("[run_sql_file] session %s: ",
                          str(sql_formatted.get('session')))
            logging.debug("[run_sql_file] description: %s",
                          str(sql_formatted.get('description')))

            logging.debug("config_file: %s DONE", str(config_file))

            return self.run_sql(sql_formatted=sql_formatted, sql_file=config_file, dry_run=dry_run)
