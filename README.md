# Employee Hub · Data Platform (Fabric workspace)

The Microsoft Fabric items for the **Employee Hub** analytics pipeline: a lakehouse, PySpark notebooks, a Data Factory pipeline, a warehouse, a semantic model and a report. They're stored as code through Fabric Git integration.

**Part of [employee-hub-fabric-app](https://github.com/namanagarwal121/employee-hub-fabric-app).** That repo holds the Employee Hub app (the data source), the [architecture](https://github.com/namanagarwal121/employee-hub-fabric-app/blob/main/data-platform/README.md) and the [setup guide](https://github.com/namanagarwal121/employee-hub-fabric-app/blob/main/data-platform/docs/setup-guide.md), and includes this repo as a submodule at `data-platform/fabric`.

![Architecture](https://raw.githubusercontent.com/namanagarwal121/employee-hub-fabric-app/main/data-platform/docs/architecture.png)

**Second source:** a live HR event stream, see the [streaming architecture](https://raw.githubusercontent.com/namanagarwal121/employee-hub-fabric-app/main/data-platform/docs/streaming-architecture.png).

## Items

| Item | Type | Role |
| --- | --- | --- |
| `lh_employee_hub` | Lakehouse | `dbo` shortcuts to the app's replicated tables, plus the `bronze` and `silver` Delta tables |
| `nb_bronze` | Notebook | Incremental + CDC load of employees: watermark with a look-back window, a row hash so identical versions aren't appended twice, delete markers (`_op = 'D'`), an automatic full load on the first run, and an optional `full_compare` mode. Refreshes departments and job titles |
| `nb_silver` | Notebook | Latest version per `id`, with deleted employees excluded. Validation **fails the run** on duplicate IDs, missing fields or broken links |
| `employee_hub_warehouse` | Warehouse | `silver` copies and the `gold` star schema. Dimension keys stay stable between runs, `DimEmployee` keeps full change history (SCD Type 2), departments and titles have `-1` "Unknown" members, `FactEmployeeDailySnapshot` builds a daily history, and both builds run inside transactions |
| `etc_pipeline` | Data pipeline | bronze → silver → copy to warehouse → `sp_DimensionTableCreation` → `sp_fctTableCreation` → refresh the semantic model |
| `sm-EmployeeHub` | Semantic model | Direct Lake model over `gold` (`DimEmployee`, `DimDepartment`, `DimJobTitle`, `FactEmployeeSnapshot`) |
| `powerBi report` | Report | Headcount and salary by department and job title |
| `es_employee_events` | Eventstream | Second source: receives live HR events from the [Python producer](https://github.com/namanagarwal121/employee-hub-fabric-app/tree/main/data-platform/streaming/producer) through a custom endpoint and lands them in `dbo.employee_events` |
| `Stream_notebook_bronze` | Notebook | Moves new landing rows into `bronze.employee_events` (one row per `event_id`, typed, flagged with a reject reason when invalid). Then writes `sql_ready.*`, the inserts, updates, deletes and audit rows the app's database still needs, compared against its current state |
| `sql_database/stream_stage_setup.sql` | SQL script | Run once in the **app's** SQL database. Creates the `stream_stage` staging tables and `sp_apply_stream_changes`, which applies the staged changes in one transaction |

## Where the source of truth lives

Fabric Git integration commits to **Azure DevOps** (`dev.azure.com/namanag1211/fabric`), and this GitHub repo is a mirror of it. To update the mirror after new Fabric commits:

```bash
git pull origin main && git push github main
```
