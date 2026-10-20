# Employee Hub · Data Platform (Fabric workspace)

The Microsoft Fabric items for the **Employee Hub** analytics pipeline: a lakehouse, PySpark notebooks, a Data Factory pipeline, a warehouse, a semantic model and a report. They're stored as code through Fabric Git integration.

**Part of [employee-hub-fabric-app](https://github.com/namanagarwal121/employee-hub-fabric-app).** That repo holds the Employee Hub app (the data source), the [architecture](https://github.com/namanagarwal121/employee-hub-fabric-app/blob/main/data-platform/README.md) and the [setup guide](https://github.com/namanagarwal121/employee-hub-fabric-app/blob/main/data-platform/docs/setup-guide.md), and includes this repo as a submodule at `data-platform/fabric`.

![Architecture](https://raw.githubusercontent.com/namanagarwal121/employee-hub-fabric-app/main/data-platform/docs/architecture.png)

## Items

| Item | Type | Role |
| --- | --- | --- |
| `lh_employee_hub` | Lakehouse | `dbo` shortcuts to the app's replicated tables, plus the `bronze` and `silver` Delta tables |
| `nb_bronze` | Notebook | Incremental load of employees (watermark on `CreatedAt` / `UpdatedAt`); refreshes departments and job titles |
| `nb_silver` | Notebook | Latest row per employee, departments and job titles, and validation checks |
| `employee_hub_warehouse` | Warehouse | `silver` copies and the `gold` star schema, built by stored procedures |
| `etc_pipeline` | Data pipeline | bronze → silver → copy to warehouse → `sp_DimensionTableCreation` → `sp_fctTableCreation` → refresh the semantic model |
| `sm-EmployeeHub` | Semantic model | Direct Lake model over `gold` (`DimEmployee`, `DimDepartment`, `DimJobTitle`, `FactEmployeeSnapshot`) |
| `powerBi report` | Report | Headcount and salary by department and job title |

## Where the source of truth lives

Fabric Git integration commits to **Azure DevOps** (`dev.azure.com/namanag1211/fabric`), and this GitHub repo is a mirror of it. To update the mirror after new Fabric commits:

```bash
git pull origin main && git push github main
```
