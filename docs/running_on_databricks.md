# A guide for other users on how to run the pipeline on Azure Databricks

I ran this end to end on Azure Databricks in October 2026, with Unity Catalog and serverless compute, as a Workflow with six notebook tasks. Setup takes around an hour and the smallest compute is enough. If you just want to try the pipeline, the local runner in the README needs no Azure account.

## 1. Azure resources

Put everything in one resource group and one region.

1. Create an Access Connector for Azure Databricks with a system-assigned managed identity, and copy its resource ID.
2. Create a storage account (Standard, LRS) with hierarchical namespace turned on. Add a container and a folder called landing.
3. On the storage account, give the access connector's managed identity the Storage Blob Data Contributor role. It can take a few minutes to take effect.


## 2. Unity Catalog objects

In Catalog Explorer, create a storage credential from the access connector, then an external location for the container. The external location has to exist before the catalog that uses it.

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

Then check that the volume can see the files:

```sql
LIST '/Volumes/ndtdata/landing/files/';
```

## 3. Load the data

```bash
python data_generator/generate_ndt_landing.py
```

Upload the contents of the generated ndt_landing folder, not the folder itself, into the landing folder of the container, and keep the sub-folders. The README and known-issues files can stay out.

If you upload from Windows through WSL, check for files ending in :Zone.Identifier. They are harmless copies that Windows adds, but delete them if you see them. The readers ignore them anyway.

## 4. Import the repo

In the workspace, choose Create, then Git folder, and paste the repository URL. The notebooks are in source format, so they open as normal notebooks.

Open notebooks/_common and look at the settings at the top:

- CATALOG is ndtdata. Change it if your catalog has another name.
- LANDING is /Volumes/ndtdata/landing/files. Change it if your catalog, schema or volume names differ.
- UT_DIR and CAL_DIR are ut-thickness-readings and calibration-records. Change them if your folders are named differently. The generator creates them with these names.

## 5. Run

Use serverless compute if you have it. Otherwise a small single-node cluster with auto-termination set to 10 or 15 minutes will do.

Run the notebooks in order, 01 to 06. Each one prints checks at the end, and notebooks 03 and 05 stop with an error if a check fails.

## 6. Recommended: run it as a Workflow/Job

Create a job with six notebook tasks, one per notebook, each depending on the one before. Running it again is safe, because notebook 04 skips batches it has already loaded.

## 7. Clean up

Stop the compute when you finish. To remove everything, delete the resource group. The managed resource group that Databricks creates for the workspace cannot be deleted directly, it disappears when the workspace goes.
