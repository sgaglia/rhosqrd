from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from sqlalchemy.sql import text
import pandas as pd
import sys
import re
import threading
import tabloo

welcome_message_text = """\n+++Welcome to RhoSquared!+++\n
This is a free open source command line database viewer
optimized for semistructured linguistic corpora.
RhoSquared has been developed as an interface to a French-Italian
morphological corpus which was built thanks to the DFG-funded
project 'Temporal analysis and modelling of the paradigmatic
extension of French and Italian verbal roots' by S. Gaglia.
It comprises both data from Old French and Old Italian and the
datasets are connected following a simple relation algebra.\n
To initialize a new query environment, type 'init qenv' below.
To display useful tips, type 'help'. To exit, type 'exit'. """

help_message_text = """Quick Help\n
This function is not fully supported in the current version. 
Please vist |--LINK--| to consult the general documentation of the project."""

class QEnv:
    """This class implements an interactive query environment"""
    query_string = []

    def __init__(self):
        self.engine = create_engine('postgresql://fritav_remote_user:Qc6o52n6H@134.76.10.93:5432/fritav')
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    def query_results_as_df(self, qdict):
        dictleton = dict()
        qdict_copy = qdict
        if "comp_dates" in qdict.keys():
            dictleton.update({"comp_dates": qdict.get("comp_dates")})
            del qdict_copy["comp_dates"]
            filter = pd.Series(qdict_copy)
            pg_table_as_df_full = pd.DataFrame()
            for key in list(self.metadata.tables.keys()):
                pg_table_as_df = pd.read_sql_table(key, con=self.engine)
                pg_table_as_df_filtered = pg_table_as_df.loc[(pg_table_as_df[list(qdict)] == filter).all(axis=1)]
                pg_table_as_df_full = pg_table_as_df_full.append(pg_table_as_df_filtered)
            ##################################
            dates = QEnv.extract_dates(self, dictleton)
            comp_dates_as_df_col = pg_table_as_df_full.comp_dates.str.extract('(\d+)')
            int_series_comp_dates = comp_dates_as_df_col.astype(int)[0].between(int(dates[0]), int(dates[1]), inclusive=False)
            return pg_table_as_df_full[int_series_comp_dates.values]
            ##################################
        elif "manuscr_dates" in qdict.keys():
            dictleton.update({"manuscr_dates": qdict.get("manuscr_dates")})
            del qdict_copy["manuscr_dates"]
            filter = pd.Series(qdict_copy)
            pg_table_as_df_full = pd.DataFrame()
            for key in list(self.metadata.tables.keys()):
                pg_table_as_df = pd.read_sql_table(key, con=self.engine)
                pg_table_as_df_filtered = pg_table_as_df.loc[(pg_table_as_df[list(qdict)] == filter).all(axis=1)]
                pg_table_as_df_full = pg_table_as_df_full.append(pg_table_as_df_filtered)
            ##################################
            dates = QEnv.extract_dates(self, dictleton)
            manuscr_dates_as_df_col = pg_table_as_df_full.manuscr_dates.str.extract('(\d+)')
            int_series_manuscr_dates = manuscr_dates_as_df_col.astype(int)[0].between(int(dates[0]), int(dates[1]), inclusive=False)
            return pg_table_as_df_full[int_series_manuscr_dates.values]
            ##################################
        else:
            pass
        filter = pd.Series(qdict)
        pg_table_as_df_full = pd.DataFrame()
        for key in list(self.metadata.tables.keys()):
            pg_table_as_df = pd.read_sql_table(key, con=self.engine)
            pg_table_as_df_filtered = pg_table_as_df.loc[(pg_table_as_df[list(qdict)] == filter).all(axis=1)]
            pg_table_as_df_full = pg_table_as_df_full.append(pg_table_as_df_filtered)
        return pg_table_as_df_full

    def extract_dates(self, qdict: dict):
        temp_qtags = ["comp_dates",
                      "manuscr_dates"]
        interval_as_str = ""
        for key, value in qdict.items():
            if key in temp_qtags:
                interval_as_str = value
        interval = list(interval_as_str[1:-1].split(",", 1))
        return interval

    def disconnect(self):
        self.connection.close()
        self.engine.dispose()
        return "Disconnected successfully."

class QStringEval:

    qtags = ["lang",
              "lemma_mod",
              "lemma_nca",
              "verb_form_dia",
              "verb_form_mod",
              "stem",
              "pos_m_features",
              "pos_m_features_alt",
              "orthogr_con",
              "m_phenom",
              "db_hit",
              "comp_dates",
              "comp_loc",
              "manuscr_dates",
              "manuscr_loc",
              "writers_dialect",
              "reg_codes_dees",
              "tok_sentence",
              "verses",
              "genres",
              "comment",
              "contrib"]

    temp_qtags = ["comp_dates",
                  "manuscr_dates"]

    def __init__(self):
        pass

    def query_string_to_dict(qstring: str):
        nspaces = qstring.replace(" ", "")
        qlist = re.split("&", nspaces)
        qdict = {}
        for qex in qlist:
            x, y = qex.split("=", 1)
            qdict[x] = y
        return qdict

    def is_valid_query_tag(self, qdict: dict):
        valid = True
        for tag in qdict:
            if tag not in self.qtags:
                print("Syntax Error: Your query string contains undefined tags. Please try again.")
                init_query_environment()
        return valid

    def extract_dates (self, qdict: dict):
        interval_as_str = ""
        for key, value in qdict.items():
            if key in self.temp_qtags:
                interval_as_str = value
        interval_as_tuple = interval_as_str[1:-1].split(",",1)
        return interval_as_tuple


def welcome_message():
    print(welcome_message_text)

def open_input_stream():
    prompt = input(">>> ")
    if prompt == 'exit':
        sys.exit()
    elif prompt == 'help':
        quick_help()
    elif prompt == 'init qenv':
        init_query_environment()
    else:
        print("Unknown command: <" + prompt + ">. Please try again or type 'help'. ")
        open_input_stream()

def quick_help():
    print(help_message_text)
    open_input_stream()

def init_query_environment():
    query_environment = QEnv()
    string_evaluator = QStringEval()
    if init_query_environment.counter == 0:
        query_prompt = input(">>> Submit a query by entering a valid query string or exit by typing 'exit qenv': ")
        if query_prompt == 'exit qenv':
            QEnv.disconnect(query_environment)
            open_input_stream()
            return
        else:
            qdict = QStringEval.query_string_to_dict(query_prompt)
            if QStringEval.is_valid_query_tag(string_evaluator, qdict):
                query_result = QEnv.query_results_as_df(query_environment, qdict)
                print("#hits = " + str(query_result.shape[0]))
                t = threading.Thread(target=tabloo.show, args=[query_result])
                t.daemon = True
                t.start()
                init_query_environment.counter += 1
                init_query_environment()
    else:
        query_prompt = input(">>> Submit a new query or type 'exit qenv' to exit the current query environment: ")
        if query_prompt == 'exit qenv':
            QEnv.disconnect(query_environment)
            open_input_stream()
            return
        else:
            qdict = QStringEval.query_string_to_dict(query_prompt)
            if QStringEval.is_valid_query_tag(string_evaluator, qdict):
                query_result = QEnv.query_results_as_df(query_environment, qdict)
                print("#hits = " + str(query_result.shape[0]))
                t = threading.Thread(target=tabloo.show, args=[query_result])
                t.daemon = True
                t.start()
                init_query_environment.counter += 1
                init_query_environment()
init_query_environment.counter = 0

if __name__ == '__main__':
    welcome_message()
    open_input_stream()
