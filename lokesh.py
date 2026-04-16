"""
Data Processing Utility Module
Includes functions for common data manipulation tasks
"""

import json
import csv
from typing import List, Dict, Any


class DataProcessor:
    """Class to handle various data processing operations"""
    
    def __init__(self, data: List[Dict[str, Any]] = None):
        """Initialize with optional data"""
        self.data = data or []
    
    def load_csv(self, filename: str) -> List[Dict[str, Any]]:
        """Load data from CSV file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.data = list(reader)
            print(f"Loaded {len(self.data)} records from {filename}")
            return self.data
        except FileNotFoundError:
            print(f"File {filename} not found")
            return []
    
    def load_json(self, filename: str) -> List[Dict[str, Any]]:
        """Load data from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            print(f"Loaded {len(self.data)} records from {filename}")
            return self.data
        except FileNotFoundError:
            print(f"File {filename} not found")
            return []
    
    def filter_data(self, key: str, value: Any) -> List[Dict[str, Any]]:
        """Filter data by key-value pair"""
        return [record for record in self.data if record.get(key) == value]
    
    def sort_data(self, key: str, reverse: bool = False) -> List[Dict[str, Any]]:
        """Sort data by a specific key"""
        return sorted(self.data, key=lambda x: x.get(key, ''), reverse=reverse)
    
    def get_unique_values(self, key: str) -> set:
        """Get unique values for a specific key"""
        return {record.get(key) for record in self.data if key in record}
    
    def to_csv(self, filename: str):
        """Export data to CSV file"""
        if not self.data:
            print("No data to export")
            return
        
        keys = self.data[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.data)
        print(f"Exported {len(self.data)} records to {filename}")
    
    def to_json(self, filename: str):
        """Export data to JSON file"""
        if not self.data:
            print("No data to export")
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2)
        print(f"Exported {len(self.data)} records to {filename}")


# Example usage
if __name__ == "__main__":
    # Create processor instance
    processor = DataProcessor()
    
    # Sample data
    sample_data = [
        {'id': 1, 'name': 'Alice', 'department': 'HR'},
        {'id': 2, 'name': 'Bob', 'department': 'IT'},
        {'id': 3, 'name': 'Charlie', 'department': 'HR'},
        {'id': 4, 'name': 'David', 'department': 'Sales'},
    ]
    
    processor.data = sample_data
    
    print("Sample Data Operations:")
    print("\nFilter HR Department:")
    for record in processor.filter_data('department', 'HR'):
        print(record)
    
    print("\nUnique Departments:")
    print(processor.get_unique_values('department'))
    
    print("\nSorted by Name:")
    for record in processor.sort_data('name'):
        print(record)
