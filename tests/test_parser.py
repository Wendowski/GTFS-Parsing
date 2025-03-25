import os
import unittest
import sqlite3
import pandas as pd
import zipfile
from gtfs_parser.parser import GTFSParser

class TestGTFSProcessor(unittest.TestCase):
    def setUp(self):
        """Set up a temporary environment for testing."""
        self.processor = GTFSParser(
            databases_dir="test_databases",
            downloads_dir="test_downloads",
            extracted_dir="test_extracted"
        )
        
        # Create necessary test directories
        os.mkdir(self.processor.databases_dir)
        os.mkdir(self.processor.downloads_dir)
        os.mkdir(self.processor.extracted_dir)
        
        # Create a test SQLite connection
        self.processor.conn = sqlite3.connect(os.path.join(self.processor.databases_dir, "test_GTFS.db"))
    
    def tearDown(self):
        """Clean up the environment after tests."""
        if os.path.exists(self.processor.databases_dir):
            for root, dirs, files in os.walk(self.processor.databases_dir):
                for file in files:
                    os.remove(os.path.join(root, file))
            os.rmdir(self.processor.databases_dir)
        
        if os.path.exists(self.processor.downloads_dir):
            os.rmdir(self.processor.downloads_dir)

        if os.path.exists(self.processor.extracted_dir):
            os.rmdir(self.processor.extracted_dir)
    
    def test_reset_directory(self):
        """Test the reset_directory method."""
        test_dir = "test_temp_dir"
        os.mkdir(test_dir)
        self.processor.reset_directory(test_dir)
        self.assertTrue(os.path.exists(test_dir))
        os.rmdir(test_dir)
    
    def test_unzip_file(self):
        """Test the unzip_file method."""
        # This requires a sample test zip file created beforehand
        test_zip = os.path.join(self.processor.downloads_dir, "test.zip")
        test_extract_to = os.path.join(self.processor.extracted_dir, "test_extracted")
        
        # Create dummy zip file for testing
        with zipfile.ZipFile(test_zip, 'w') as zip_ref:
            zip_ref.writestr("dummy.txt", "This is a test file.")

        self.processor.unzip_file(test_zip, test_extract_to)
        self.assertTrue(os.path.exists(os.path.join(test_extract_to, "dummy.txt")))
    
    def test_add_file_to_database(self):
        """Test adding a file to the database."""
        test_table_name = "test_table"
        data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        test_df = pd.DataFrame(data)
        test_file = os.path.join(self.processor.extracted_dir, "test_file.csv")
        test_df.to_csv(test_file, index=False)

        self.processor.add_file_to_database(test_file, test_table_name, "test_release")
        
        cursor = self.processor.conn.execute(f"SELECT * FROM {test_table_name}")
        columns = [desc[0] for desc in cursor.description]
        self.assertIn("col1", columns)
        self.assertIn("col2", columns)
        self.assertIn("release_name", columns)
    
    def test_fix_missing_timepoint(self):
        """Test fixing the missing timepoint column."""
        test_table_name = "stop_times"
        self.processor.conn.execute(f"CREATE TABLE {test_table_name} (col1 INTEGER)")
        self.processor.fix_missing_timepoint()

        cursor = self.processor.conn.execute(f"SELECT * FROM {test_table_name}")
        columns = [desc[0] for desc in cursor.description]
        self.assertIn("timepoint", columns)
    
    def test_process_release(self):
        """Test the process_release method with dummy data."""
        # Requires dummy test zip and GTFS files for a complete test
        pass  # This would need more preparation to set up a complete test case

if __name__ == "__main__":
    unittest.main()
