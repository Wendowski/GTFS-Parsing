from gtfs_parser.parser import GTFSParser

def main():
    # Create an instance of the GTFSParser
    processor = GTFSParser(
        databases_dir="databases",   # Directory for the SQLite database
        downloads_dir="downloads",  # Directory containing the GTFS zip files
        extracted_dir="extracted"   # Directory to extract GTFS files
    )

    # Run the processing workflow
    # Currently generates a SQLite database of all the GTFS releases downloaded.
    processor.run()


if __name__ == "__main__":
    main()
