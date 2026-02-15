def initialize_metadata(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            layer TEXT,
            city TEXT,
            date DATE,
            processed_at TIMESTAMP,
            PRIMARY KEY (layer, city, date)
        );
    """)