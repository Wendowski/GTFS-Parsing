import os
import glob
import sqlite3
import pandas as pd
import shutil
from zipfile import ZipFile


class GTFSParser:
    def __init__(self, databases_dir="database", downloads_dir="downloads", extracted_dir="extracted"):
        self.databases_dir = databases_dir
        self.database_path = os.path.join(databases_dir, "GTFS.db")
        self.downloads_dir = downloads_dir
        self.extracted_dir = extracted_dir
        self.conn = None

    def reset_directory(self, directory):
        """Delete and recreate a directory."""
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.mkdir(directory)

    def unzip_file(self, zip_path, extract_to):
        """Unzip a file to a specific location."""
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def process_release(self, release):
        """Process a single GTFS release and its feeds."""
        release_name = os.path.basename(release).replace(".zip", "")
        print(f"Unzipping release: {release_name}...")
        release_path = os.path.join(self.extracted_dir, release_name)
        self.unzip_file(release, release_path)

        feeds = glob.glob(os.path.join(release_path, "*.zip"))
        for feed in feeds:
            feed_name = os.path.splitext(feed)[0]  # Remove ".zip" extension
            print(f"Unzipping GTFS feed: {feed_name}...")
            self.unzip_file(feed, feed_name)

            files = glob.glob(os.path.join(feed_name, "*.txt"))
            for file in files:
                table_name = os.path.basename(file).replace(".txt", "")
                print(f"Processing file: {table_name} for feed: {feed_name}...")
                self.add_file_to_database(file, table_name, release_name)

    def add_file_to_database(self, file, table_name, release_name):
        """Add a file to the database as a new table or append to existing table."""
        table = pd.read_csv(file, skipinitialspace=True)

        cursor = self.conn.execute(
            f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        if cursor.fetchone()[0] == 1:  # Table already exists
            existing_columns = [desc[0] for desc in self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1").description]
            for column in existing_columns:
                if column not in table.columns:
                    table[column] = ""  # Add missing columns to the DataFrame
            for column in table.columns:
                if column not in existing_columns:
                    self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT")
                    self.conn.commit()

        table['release_name'] = release_name
        table.to_sql(table_name, self.conn, if_exists='append', index=False)

    def fix_missing_timepoint(self):
        """Fix missing 'timepoint' field in stop_times table."""
        cursor = self.conn.execute("SELECT * FROM stop_times LIMIT 1")
        existing_columns = [desc[0] for desc in cursor.description]
        if "timepoint" not in existing_columns:
            print("Adding 'timepoint' column to stop_times...")
            self.conn.execute("ALTER TABLE stop_times ADD COLUMN timepoint TEXT")
            self.conn.commit()

    def process_downloads(self):
        """Process all downloaded releases."""
        print("Processing downloaded releases...")
        releases = glob.glob(os.path.join(self.downloads_dir, "*.zip"))
        for release in releases:
            self.process_release(release)

    def run(self):
        """Main entry point for processing."""
        # Reset directories
        self.reset_directory(self.databases_dir)
        self.reset_directory(self.extracted_dir)

        print(f"Opening database: {self.database_path}")
        self.conn = sqlite3.connect(self.database_path)

        self.process_downloads()

        print("Fixing missing 'timepoint' field in stop_times...")
        self.fix_missing_timepoint()

        self.conn.close()
