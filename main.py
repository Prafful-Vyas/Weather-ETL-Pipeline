import duckdb
import logging
from metadata import initialize_metadata
import bronze
import silver
import gold

logging.basicConfig(level=logging.INFO)

def main():
    con = duckdb.connect("pipeline.duckdb")

    initialize_metadata(con)

    bronze.run(con)
    silver.run(con)
    gold.run(con)

if __name__ == "__main__":
    main()
