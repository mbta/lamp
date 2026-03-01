Generic single-database configuration.

# Regeneration instructions

1. run migration_template_generator.py
2. fill it in with the migration instructions. 
3. deploy this to performance manager and ingestion
    - ingestion updates Metadata table
    - performance manager updates Data Tables

    It is allowable to put both metadata and prod db migrations in a single prod migration script. 

    This is valid because ingestion deploys first (e.g. resetting metadata "finished processing" flags)
    Then performance manager deploys (e.g. clearing old data)
    Then performance manager checks if there is work (there is now, bc metadata is rest) and starts to re-process and fill in days (which are empty b/c they were cleared)

4. verify that the migration has worked (either by looking at updated_date and the reset-metadata flags on the database, or running another adhoc query to check those...)

5. in a new PR, bump version of the tableau upload file to regenerate the whole dataset (otherwise we only grab daily incremental changes)