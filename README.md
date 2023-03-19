# Pyper

An (experimental) ETL toolkit based on [DuckDB](https://duckdb.org/) and [PRQL](https://prql-lang.org/).

## Usage

Pyper relies on YAML to describe workflows. We use pydantic to model how a workflow file should look. Here's an example that showcases a simple workflow:

```yaml
# myworkflow.yaml
extract:
  provider: local
  uri: file:///mnt/ssd/projects/pyper/invoices.csv
  register: my_data_source

transform:
  lang: prql
  backend: duckdb
  query: |
    from my_data_source
    filter billing_country == "USA"
    group [customer_id] (
      aggregate [
        sum total,
        count,
      ]
    )

load:
  provider: local
  uri: file:///mnt/ssd/projects/pyper/invoices_usa.csv
```

Then, using Python:

```python
import pyper
pyper.workflow('myworkflow.yaml').exec()
```
