import pandas as pd
import datetime
import os
from pathlib import Path
from typing import List, Optional

class DataMatcher:
    def __init__(self, 
                 monday_data_path: str,
                 calendly_ceint_path: str, 
                 calendly_ce_path: str,
                 output_dir: str):
        """Initialize paths and create output directory if needed"""
        self.monday_data_path = monday_data_path
        self.calendly_ceint_path = calendly_ceint_path
        self.calendly_ce_path = calendly_ce_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_monday_data(self, start_date: str, end_date: str, sort_columns: List[str]) -> pd.DataFrame:
        """Load and filter Monday.com data"""
        df = pd.read_csv(self.monday_data_path)
        return df[(df[sort_columns[0]] >= start_date) & (df[sort_columns[0]] <= end_date)]

    def load_calendly_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Load and combine Calendly data from multiple sources"""
        df_ceint = pd.read_csv(self.calendly_ceint_path)
        df_ce = pd.read_csv(self.calendly_ce_path)
        
        # Combine datasets
        df_combined = pd.concat([df_ceint, df_ce])
        
        # Convert event_start to datetime
        df_combined['event_start'] = pd.to_datetime(df_combined['event_start'])
        
        # Filter by date range
        return df_combined[
            (df_combined['event_start'] >= start_date) & 
            (df_combined['event_start'] <= end_date)
        ]

    def match_data(self, monday_df: pd.DataFrame, calendly_df: pd.DataFrame) -> pd.DataFrame:
        """Match Monday.com and Calendly data based on email"""
        return pd.merge(
            calendly_df,
            monday_df,
            left_on='invitee_email',
            right_on='Email',
            how='inner'
        )

    def save_results(self, df: pd.DataFrame) -> str:
        """Save matched results to CSV file"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.output_dir / f'df_matched_emails_{timestamp}.csv'
        df.to_csv(output_path, index=False)
        return str(output_path)

def main():
    # Configuration
    DATA_PATHS = {
        'monday': '/Users/code.ai/Cleverly/Calendly/Calendly_API/data/downloads_monday/Monday_Data_20250625_024626.csv',
        'calendly_ceint': '/Users/code.ai/Cleverly/Calendly/Calendly_API/data/downloads/invitees_cleverly-introduction-cold-email-international.csv',
        'calendly_ce': '/Users/code.ai/Cleverly/Calendly/Calendly_API/data/downloads/invitees_cleverly-introduction-cold-email.csv'
    }
    OUTPUT_DIR = '/Users/code.ai/Cleverly/Calendly/Calendly_API/data/transformations'
    DATE_RANGE = {
        'start': '2025-06-01',
        'end': '2025-06-25'
    }
    SORT_COLUMNS = ['Date Created', 'Sales Call Date']

    try:
        # Initialize matcher
        matcher = DataMatcher(
            DATA_PATHS['monday'],
            DATA_PATHS['calendly_ceint'],
            DATA_PATHS['calendly_ce'],
            OUTPUT_DIR
        )

        # Process data
        monday_df = matcher.load_monday_data(
            DATE_RANGE['start'], 
            DATE_RANGE['end'],
            SORT_COLUMNS
        )
        
        calendly_df = matcher.load_calendly_data(
            DATE_RANGE['start'],
            DATE_RANGE['end']
        )

        # Match and save results
        matched_df = matcher.match_data(monday_df, calendly_df)
        output_path = matcher.save_results(matched_df)
        
        print(f"Data successfully processed and saved to: {output_path}")

    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    main()
