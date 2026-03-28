DuckDB versions must match across CLI / Python / dbt

h3 must be installed:

INSTALL h3 FROM community;

dbt requires:

+pre-hook: "LOAD h3"
Extensions are session-based → must be loaded in dbt