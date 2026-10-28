# Running the pipeline on Azure Databricks

## 1. Azure resources

Create these in one resource group and one region, so you can delete everything in one step later.

1. **Access Connector for Azure Databricks** with a system-assigned managed identity. Copy its resource ID.
2. **Storage account** (Standard, LRS) with **hierarchical namespace enabled**. Add a container and a `landing` folder.
3. **Role assignment** on the storage account: `Storage Blob Data Contributor`, assigned to the access connector's managed identity. Wait a few minutes for it to take effect.

You also need a Databricks workspace on the Premium tier with a Unity Catalog metastore attached.

## 2. Unity Catalog objects

In Catalog Explorer, create a storage credential from the access connector, then an external location for the container. Order matters: the external location must exist before the catalog that uses it.

```sql
CREATE CATALOG IF NOT EXISTS ndtdata
MANAGED LOCATION 'abfss://<container>@<storageaccount>.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS ndtdata.landing;
CREATE SCHEMA IF NOT EXISTS ndtdata.bronze;
CREATE SCHEMA IF NOT EXISTS ndtdata.silver;
CREATE SCHEMA IF NOT EXISTS ndtdata.gold;

CREATE EXTERNAL VOLUME IF NOT EXISTS ndtdata.landing.files
LOCATION 'abfss://<container>@<storageaccount>.dfs.core.windows.net/landing';
```

Check it:

```sql
LIST '/Volumes/ndtdata/landing/files/';
```

## 3. Load the data

```bash
python data_generator/generate_ndt_landing.py
```

Upload the contents of the generated `ndt_landing` folder (not the folder itself) into the `landing` folder of the container, keeping the sub-folders. The README and known-issues files in it can be left out.

## 4. Import the repo

Workspace, Create, Git folder, and paste the repository URL. The notebooks are in source format, so they open as notebooks.

Open `notebooks/_common` and check the configuration at the top:

| Setting | Default | Change it if |
| --- | --- | --- |
| `CATALOG` | `ndtdata` | your catalog has another name |
| `LANDING` | `/Volumes/ndtdata/landing/files` | your catalog, schema or volume names differ |
| `UT_DIR`, `CAL_DIR` | `ut-thickness-readings`, `calibration-records` | your folders have other names. The generator creates them with these names |

## 5. Run

Use serverless compute if your workspace offers it, otherwise a small single-node cluster with auto-termination set to 10 to 15 minutes.

Run in order: `01`, `02`, `03`, `04`, `05`, `06`. Each notebook prints checks at the end. Notebooks `03` and `05` stop with an error if a check fails.

## 6. Optional: run it as a Workflow

Create a job with six tasks, each a notebook task pointing at one notebook, with every task depending on the one before it. Run it once and keep a screenshot of the successful run. Re-running the job is safe: notebook `04` skips batches it has already loaded.

## 7. Clean up

Stop compute when you finish. When you no longer need the project, delete the resource group to remove every Azure resource, and with it any ongoing cost.
