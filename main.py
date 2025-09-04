import asyncio
import argparse
import logging

# Set up logging for the main entry point
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Parses command-line arguments and runs the requested task."""
    parser = argparse.ArgumentParser(description="L'Oréal Project Data Pipeline CLI")
    
    # Argument to set up the database
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize the database, create tables, and set up hypertables."
    )
    
    # Argument to run the AI enrichment
    parser.add_argument(
        "--enrich-comments",
        action="store_true",
        help="Run the AI enrichment pipeline on the comments table."
    )

    args = parser.parse_args()

    if args.init_db:
        from src.init_db import bootstrap_database
        logging.info("Starting database initialization...")
        asyncio.run(bootstrap_database())

    elif args.enrich_comments:
        from src.tasks.enrich_comments import run_enrichment_pipeline
        logging.info("Starting comment enrichment pipeline...")
        asyncio.run(run_enrichment_pipeline())
        
    else:
        print("No task specified. Use --help to see available options.")

if __name__ == "__main__":
    main()