import sys
import os

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path + "\\")

import asyncio as a_io
import pandas as pd
import OpenBeerDb as obd
import matplotlib.pyplot as plt

MSG_QUERY: str = "Enter sql statements below or press enter to show the queries results"
MSG_FREQ_CHART: str = "Enter the sql statement, the name of the observed column\n and the label for the x axis"
MSG_INPUT_COL: str = "enter column: "
MSG_INPUT_KEY: str = "enter a key: "
MSG_INPUT_LABEL_X: str = "enter label x axis: "
MSG_INPUT_MENU: str = "enter any key to return to menu or (x) for exit : "
MSG_INPUT_SQL: str = "enter sql: "
MSG_INPUT_TBL: str = "enter table: "
MNU_EXIT: str = "Exit"
MNU_CHART_FREQ: str = "Frequency Chart"
MNU_QUERIES: str = "Queries"
PRE_TITLE: str = "Open Beer Presenter"

class Presenter:
    def __init__(self, title=PRE_TITLE, max_len=40, menu=None):
        self._loop = a_io.get_event_loop()
        self.menu: dict = menu
        self.title = title
        self.max_len = max_len

    @staticmethod
    def center_text(text, max_len): return f"{' ' * int(max_len / 2 - len(text[:max_len]) / 2)}{text[:max_len]}"

    def show_menu(self):
        print(f"\n{'_' * self.max_len}\n\n{Presenter.center_text(self.title, self.max_len)}\n")
        for key in sorted(self.menu.keys()): print(f"\t{key}\t{self.menu[key]}")
        print(f"\n{'_' * self.max_len}\n")
        key = input(MSG_INPUT_KEY)
        return key

class OpenBeerPresenter(Presenter):
    def __init__(self):
        self.openBeer = obd.OpenBeerDb(in_memory=False, show_import_info=False)
        super().__init__(max_len=80, menu={
            "q": MNU_QUERIES,
            "f": MNU_CHART_FREQ,
            "x": MNU_EXIT
        })

    def present(self):
        key = self.show_menu()
        if not key in self.menu:
            self.present()
        elif self.menu[str(key).lower()] == MNU_CHART_FREQ:
            OpenBeerPresenter.show_freq_chart()
        elif self.menu[str(key).lower()] == MNU_QUERIES:
            self.show_queries()
            self.nav_to_mnu()
        elif str(key).lower() == "x":
            exit(0)
            return

    def nav_to_mnu(self):
        print()
        n_key = input(MSG_INPUT_MENU)
        exit(0) if str(n_key).lower() == "x" else self.present()

    @staticmethod
    def show_freq_chart():
        print(f"\n{MSG_FREQ_CHART}\n")
        sql = input(MSG_INPUT_SQL)
        col = input(MSG_INPUT_COL)
        x_lbl = input(MSG_INPUT_LABEL_X)
        if sql and col:
            df = obd.Data.query(sql)
            df.plot(kind="hist", bins=30)
            plt.xlabel(x_lbl if x_lbl else col)

    def show_queries(self):
        print(f"\n{MSG_QUERY}\n")
        sql_list = []
        sql = None
        while sql != "":
            sql = input(MSG_INPUT_SQL)
            if len(sql) > 0: sql_list.append(sql)
        if sql_list:
            dfs = self.openBeer.query_async(sql_list)
            for df in dfs:
                pd.set_option("display.max_columns", None)
                pd.set_option("display.max_rows", 1000)
                pd.set_option("display.max_colwidth", 30)
                print(f"\n{df}\n")


if __name__ == '__main__':
    OpenBeerPresenter().present()
    # select b.name, c.cat_name category from beers b left join categories c on b.cat_id = c.id where cast(b.cat_id as integer) >= 0
