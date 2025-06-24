from .monday_extract_groups import fetch_items_recursive, fetch_groups
import os
import sys
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
from tqdm import tqdm
import dotenv
import datetime
import argparse
from pathlib import Path


dotenv.load_dotenv()


@dataclass
class MondayConfig:
    """Configuration for Monday.com API"""
    BOARD_ID: str = "6942829967"
    ITEMS_LIMIT: int = 500
    GROUP_MAPPING = {
        "topics": "scheduled",
        "new_group34578__1": "unqualified", 
        "new_group27351__1": "won",
        "new_group54376__1": "cancelled",
        "new_group64021__1": "noshow",
        "new_group65903__1": "proposal",
        "new_group62617__1": "lost"
    }
    COLUMN_MAPPING = {
        'name': 'Name',
        'auto_number__1': 'Auto number',
        'person': 'Owner',
        'last_updated__1': 'Last updated',
        'link__1': 'Linkedin',
        'phone__1': 'Phone',
        'email__1': 'Email',
        'text7__1': 'Company',
        'date4': 'Sales Call Date',
        'status9__1': 'Follow Up Tracker',
        'notes__1': 'Notes',
        'interested_in__1': 'Interested In',
        'status4__1': 'Plan Type',
        'numbers__1': 'Deal Value',
        'status6__1': 'Email Template #1',
        'dup__of_email_template__1': 'Email Template #2',
        'status__1': 'Deal Status',
        'status2__1': 'Send Panda Doc?',
        'utm_source__1': 'UTM Source',
        'date__1': 'Deal Status Date',
        'utm_campaign__1': 'UTM Campaign',
        'utm_medium__1': 'UTM Medium',
        'utm_content__1': 'UTM Content',
        'link3__1': 'UTM LINK',
        'lead_source8__1': 'Lead Source',
        'color__1': 'Channel FOR FUNNEL METRICS',
        'subitems__1': 'Subitems',
        'date5__1': 'Date Created'
    }

class MondayDataProcessor:
    """Class to handle Monday.com data processing"""
    
    def __init__(self, config: MondayConfig):
        self.config = config
        self.api_key = self._get_api_key()

    @staticmethod
    def _get_api_key() -> str:
        """Fetch API key from environment variables"""
        api_key = os.getenv("MONDAY_API_KEY")
        if not api_key:
            raise ValueError("MONDAY_API_KEY not found in environment variables")
        return api_key

    def _items_to_dataframe(self, items: List[dict]) -> pd.DataFrame:
        """Convert Monday.com items to pandas DataFrame"""
        if not items:
            print("No items to convert.")
            return pd.DataFrame()
        
        # Check if the first item has column_values to avoid index error
        if not items[0].get('column_values'):
            print("No column values found in items.")
            return pd.DataFrame()
        
        data = []
        column_ids = [column['id'] for column in items[0]['column_values']]
        headers = ['Item ID', 'Item Name'] + column_ids

        for item in items:
            row = {
                'Item ID': item['id'],
                'Item Name': item['name']
            }
            # Safely handle column values
            if 'column_values' in item:
                for column in item['column_values']:
                    row[column['id']] = column.get('text', '')
            data.append(row)
        
        return pd.DataFrame(data, columns=headers)

    def _process_group(self, group_id: str, group_name: str, groups: List[dict]) -> Optional[pd.DataFrame]:
        """Process a single group and return its DataFrame"""
        target_group = next((group for group in groups if group['id'] == group_id), None)
        if not target_group:
            print(f"Group with ID '{group_id}' not found in board {self.config.BOARD_ID}.")
            return None
            
        print(f"Fetching items from Group: **{target_group['title']}** (ID: {target_group['id']})")
        
        try:
            items = fetch_items_recursive(
                self.config.BOARD_ID, 
                target_group['id'], 
                self.api_key, 
                self.config.ITEMS_LIMIT
            )
            df = self._items_to_dataframe(items)
            df.rename(columns=self.config.COLUMN_MAPPING, inplace=True)
            return df
        except Exception as e:
            print(f"Error fetching items for group '{group_name}': {e}")
            return None

    def fetch_data(self) -> Dict[str, pd.DataFrame]:
        """Main method to fetch and process all Monday.com data"""
        try:
            groups = fetch_groups(self.config.BOARD_ID, self.api_key)
        except Exception as e:
            raise RuntimeError(f"Error fetching groups: {e}")

        dataframes = {}
        
        # Using tqdm correctly for progress bar
        for group_id, group_name in tqdm(
            self.config.GROUP_MAPPING.items(),
            desc="Fetching Groups",
            total=len(self.config.GROUP_MAPPING)
        ):
            df = self._process_group(group_id, group_name, groups)
            if df is not None:
                dataframes[group_name] = df

        return dataframes

def parse_args() -> argparse.Namespace:
    """Parse and validate command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download and process Monday.com data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./data/downloads_monday',
        help='Directory path to save the downloaded data'
    )
    
    parser.add_argument(
        '--board-id',
        type=str,
        help='Monday.com board ID to fetch data from (overrides default in MondayConfig)'
    )
    
    parser.add_argument(
        '--items-limit',
        type=int,
        help='Maximum number of items to fetch per group (overrides default in MondayConfig)'
    )

    args = parser.parse_args()

    # Validate output directory
    os.makedirs(args.output_dir, exist_ok=True)
    if not os.access(args.output_dir, os.W_OK):
        parser.error(f"Output directory '{args.output_dir}' is not writable")

    return args

def main():
    """Main execution function"""
    args = parse_args()
    
    # Create config with potential CLI overrides
    config = MondayConfig()
    if args.board_id:
        config.BOARD_ID = args.board_id
    if args.items_limit:
        config.ITEMS_LIMIT = args.items_limit
    
    processor = MondayDataProcessor(config)
    
    try:
        dataframes = processor.fetch_data()
        
        # Check if we have any data to process
        if not dataframes:
            print("No data retrieved from Monday.com. Exiting.")
            return
        
        # Create master dataframe with proper group assignment
        master_data = []
        for group_name, df in dataframes.items():
            if not df.empty:
                # Add Group column to each dataframe before concatenating
                df_copy = df.copy()
                df_copy['Group'] = group_name
                master_data.append(df_copy)
        
        if not master_data:
            print("No valid data to save. All groups returned empty results.")
            return
            
        master_df = pd.concat(master_data, ignore_index=True)
        
        # Generate output filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'Monday_Data_{timestamp}.csv'
        output_path = os.path.join(args.output_dir, filename)
        
        # Save to CSV
        master_df.to_csv(output_path, index=False)
        print(f"Data successfully saved to: {output_path}")

    except Exception as e:
        print(f"Error in data processing: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
