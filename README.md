# Python-Based High Volume Data Reconciliation Utility - User Guide & Reference

## Purpose
The Python-based reconciliation utility is designed to compare and reconcile high-volume structured data sets between two systems, which could be files, databases, or a combination of both. This tool provides a systematic and efficient approach to data reconciliation by identifying discrepancies between source and target data sets.

## Key Features
- **File to File Comparison**: Supports comparison of various file formats including CSV, XLSX, TXT, JSON, XML, and fixed-width files.
- **Database to Database Comparison**: Compatible with multiple database systems such as Oracle, MSSQL, MySQL, Azure Data Explorer (ADX), DB2, Cosmos DB, etc.
- **Cross-Combination Support**: Enables file-to-database and database-to-file comparisons.
- **Key-Based Comparison**: Supports reconciliation based on primary keys or composite primary keys.
- **Index-Based Comparison**: Allows row-by-row comparison when key-based reconciliation is not applicable.
- **Numeric Tolerance Handling**: Provides flag-based tolerance for numeric fields to account for precision differences.
- **String/Space Tolerance Handling**: Offers flag-based space tolerance for text/string fields to ignore unnecessary whitespace variations.
- **Detailed Diff Reports**: Generates reconciliation results in HTML and XLSX formats for easy review.
- **Parallel Diff Visualization**: Displays unmatched attributes/fields side by side for quick analysis.
- **Row and Column Level Statistics**: Provides detailed discrepancy statistics at both row and column levels.
- **Scalability & Performance Optimization**: Designed to handle large data volumes efficiently.
- **Customizable Flags**: Users can enable/disable specific reconciliation features via configuration settings.

## How It Works
### Configuration Setup
1. Define the data sources (Source name, Target name,file paths, database connections) in `Driver_Config.xlsx`.
2. Specify keys or candidate keys ( for key based compares ) or index-based comparisons ( row by row compares ).
3. Specify other recon flags e.g.

### Execution
1. Run the Python utility, which reads the configuration and initiates the reconciliation process.
2. The tool loads data from both source and target into dataframes, compares them, and identifies discrepancies along with many compare insights
3. Visualize diffs by analysing recon reports. 

### Results & Reporting
- Generates a detailed reconciliation report highlighting matched and mismatched records.
- Provides summary statistics for quick analysis.
  - Row Statistics
  - Column Statistics
  - Count Statistics
- Outputs differences in an easy-to-review format.

## Benefits
- **Saves Time & Effort**: Automates the data reconciliation process, reducing manual effort.
- **Comprehenssive Data Comparison**: Complete Data certification for data migration / comparison between 2 layers of system.
- **Ensures Data Integrity**: Helps in identifying missing, mismatched, or incorrect data between systems.
- **Configurable & Adaptable**: Allows users to define custom reconciliation rules without modifying the code.
- **Supports Large Data Sets**: Efficiently processes high-volume data, making it suitable for enterprise use.
- **Actionable Insights**: Provides detailed insights into discrepancies, helping in quick resolution.

## Getting Started
### Prerequisites
- Python 3.x installed
- Required dependencies (install via `pip install -r requirements.txt`)
- Access to source and target systems (file paths, database credentials, etc.)

### Running the Utility
1. Prepare the `Driver_Config.xlsx` file with necessary inputs.
2. Run the script:
   ```bash
   python recon_utility.py
   ```
3. OR run through pycharm. 
4. Review the output in the recon report ( html or text or xlsx ).

## Conclusion
This Python-based reconciliation utility is an essential tool for validating data consistency across systems. With its configurable setup, robust processing capabilities, and detailed reporting, it simplifies the reconciliation process and enhances data accuracy in enterprise environments.
