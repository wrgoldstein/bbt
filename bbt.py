import graphlib
import pathlib
import re
from typing import List

import fire
import jinja2
import sql_metaparse as sm


env = jinja2.Environment(loader=jinja2.PackageLoader("bbt", ""))

def config(**kwargs):
    """
    A `config` block at the top of a SQL file will allow for arbitrary
    options to be set. It's not currently used.
    """
    print(f":: Would set some configuration with {kwargs}")
    return ""


def qualify_table_path(table: str):
    if re.match('"\w+"\."\w+"', table):
        return table
    schema, table = table.split(".")
    return f'"{schema}"."{table}"'


def gather(path: str):
    return list(pathlib.Path(path).glob("**/*.sql"))


def parse(files: List[pathlib.Path]) -> list:
    nodes = {}
    graph = {}

    for f in files:
        node = qualify_table_path(f.stem)
        graph[node] = set()
        meta = sm.parse_meta(f.read_text())
        nodes[node] = dict(path=f, **meta)
        for table in meta["tables"]:
            graph[node].add(table)
    
    ts = graphlib.TopologicalSorter(graph)
    return nodes, list(ts.static_order())


def run(selector):
    files = gather(selector)
    nodes, run_order = parse(files)

    for n in run_order:
        # The actual running is just getting a psycopg2 connection
        # and doing an atomic table swap or an append depending on
        # table metadata
        print(f"\nWould run {n}: ")
        posix = nodes[n]["path"].as_posix()
        template = env.get_template(posix)
        template.globals['config'] = config
        print(template.render().strip())

if __name__ == "__main__":
    fire.Fire()
