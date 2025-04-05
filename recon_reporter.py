import pandas as pd
import datetime
from logger_config import logger  # Import global logger


class ReconReportGenerator:
    def __init__(self, comparison, source_data, target_data, config):
        self.comparison = comparison
        self.source_data = source_data
        self.target_data = target_data
        self.config = config

    def recon_report(self):
        """Main method that generates the reconciliation report."""
        summary_data = self.generate_summary_stats()
        duplicate_flags = self.detect_duplicates()
        styled_html = self.generate_styled_html()
        filename = self.generate_report_filename()

        template_content = self.load_template()
        populated_report = self.populate_template(template_content, summary_data, duplicate_flags, styled_html)

        self.save_html_report(filename, populated_report)
        self.save_xlsx_reports("./resources/recon_reports/xlsx/comprehensive_recon_report.xlsx")

    def generate_summary_stats(self):
        df_col_stats = self.comparison.column_stats
        summary = {
            "src_row_count": self.source_data.shape[0],
            "src_col_count": len(self.source_data.columns),
            "tgt_row_count": self.target_data.shape[0],
            "tgt_col_count": len(self.target_data.columns),
            "common_rows_count": self.comparison.intersect_rows.shape[0],
            "rows_in_src_only": self.comparison.df1_unq_rows.shape[0],
            "rows_in_tgt_only": self.comparison.df2_unq_rows.shape[0],
            "rows_having_mismatch": self.comparison.all_mismatch().shape[0],
            "row_having_no_mismatch": self.comparison.intersect_rows.shape[0] - self.comparison.all_mismatch().shape[0],
            "common_cols_count": len(self.comparison.intersect_columns()),
            "cols_in_src_only_count": len(self.comparison.df1_unq_columns()),
            "cols_in_tgt_only_count": len(self.comparison.df2_unq_columns()),
            "cols_having_no_mismatch": sum(1 for col in df_col_stats if col['all_match']),
            "cols_having_mismatch": sum(1 for col in df_col_stats if not col['all_match']),
            "absolute_tole": str(self.comparison.abs_tol),
            "relative_tole": str(self.comparison.rel_tol),
            "matched_keys": str(self.comparison.join_columns),
            "spaces_ignored": self.comparison.ignore_spaces
        }
        return summary

    def detect_duplicates(self):
        comparison_keys_lower = [key.lower() for key in self.config['comparison_keys']]
        dupe_row_in_src_count = 0
        dupe_row_in_tgt_count = 0

        if not self.comparison.df1_unq_rows.empty:
            dupe_row_in_src = pd.merge(self.comparison.df1_unq_rows, self.comparison.intersect_rows,
                                       on=comparison_keys_lower, how='inner')
            dupe_row_in_src_count = dupe_row_in_src.shape[0]

        if not self.comparison.df2_unq_rows.empty:
            dupe_row_in_tgt = pd.merge(self.comparison.df2_unq_rows, self.comparison.intersect_rows,
                                       on=comparison_keys_lower, how='inner')
            dupe_row_in_tgt_count = dupe_row_in_tgt.shape[0]

        return {
            "duplicate_flag_src": 'Yes' if dupe_row_in_src_count > 0 else 'No',
            "duplicate_flag_tgt": 'Yes' if dupe_row_in_tgt_count > 0 else 'No'
        }

    def generate_styled_html(self):
        """Generates an HTML-styled DataFrame highlighting mismatches."""
        all_mismatch = self.comparison.all_mismatch()
        if all_mismatch.shape[0] >500:
            logger.info("Mismatched rows are greater than 500 rows. please refer xlsx report to analyse detailed differences...")
        styled_df = all_mismatch.head(500).style.apply(self.highlight_diff, axis=1)
        return styled_df.hide(axis=0)._repr_html_()

    def generate_styled_diff_df(self):
        """Generates styled DataFrame highlighting mismatches."""
        all_mismatch = self.comparison.all_mismatch()
        styled_df = all_mismatch.head(500).style.apply(self.highlight_diff, axis=1)
        return styled_df

    def generate_report_filename(self):
        now = datetime.datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H_%M_%S")
        return f"./resources/recon_reports/html/recon_report_{self.config['source_name']}_{self.config['target_name']}_{date_str}_{time_str}.html"

    def load_template(self):
        """Loads the HTML template for the reconciliation report."""
        with open('./resources/report_tmpl/recon_report_tmpl.html', 'r') as file:
            return file.read()

    def populate_template(self, template, summary, duplicate_flags, styled_html):
        """Replaces placeholders in the HTML template with actual data."""
        df_col_with_uneq_values_types = pd.DataFrame.from_dict(self.comparison.column_stats)

        replacements = {
            "#absolute_tole#": summary["absolute_tole"],
            "#relative_tole#": summary["relative_tole"],
            "#matched_keys#": summary["matched_keys"],
            "#Columns with un-eq values / types#": df_col_with_uneq_values_types.to_html(),
            "#Rows with un-eq values#": styled_html,
            "#Rows Only In Src#": self.comparison.df1_unq_rows.to_html(),
            "#Rows Only In Tgt#": self.comparison.df2_unq_rows.to_html(),
            "#Summary Chart#": str([summary["src_row_count"], summary["src_col_count"],
                                    summary["tgt_row_count"], summary["tgt_col_count"]]),
            "#Row Summary#": str([summary["common_rows_count"], summary["rows_in_src_only"],
                                  summary["rows_in_tgt_only"], summary["row_having_no_mismatch"],
                                  summary["rows_having_mismatch"]]),
            "#Column Summary#": str([summary["common_cols_count"], summary["cols_in_src_only_count"],
                                     summary["cols_in_tgt_only_count"], summary["cols_having_no_mismatch"],
                                     summary["cols_having_mismatch"]]),
            "#duplicate_flag_src#": duplicate_flags["duplicate_flag_src"],
            "#duplicate_flag_tgt#": duplicate_flags["duplicate_flag_tgt"],
            "#spaces_ignored#": str(summary["spaces_ignored"])
        }

        for key, value in replacements.items():
            template = template.replace(key, value)

        return template

    def save_html_report(self, filename, content):
        with open(filename, 'w') as file:
            file.write(content)

    def highlight_diff(self, row):
        """Highlights mismatched cells."""
        styles = [''] * len(row)
        for i in range(len(self.config['comparison_keys']), len(row)-1, 2):
            if row.iloc[i] != row.iloc[i + 1]:
                styles[i] = 'background-color: #FF6347'
                styles[i + 1] = 'background-color: #FF6347'
        return styles

    def save_xlsx_reports(self, output_path):
        try:
            all_mismatch = self.comparison.all_mismatch()
            styled_diff_dataframe = self.generate_styled_diff_df()
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                if len(all_mismatch)>0:
                    styled_diff_dataframe.to_excel(writer, sheet_name='all_mismatched_rows', index=False)
                if hasattr(self.comparison, 'df1_unq_rows') and len(self.comparison.df1_unq_rows)>0:
                    self.comparison.df1_unq_rows.to_excel(writer, sheet_name='src_unique_rows', index=False)
                if hasattr(self.comparison, 'df2_unq_rows') and len(self.comparison.df2_unq_rows)>0:
                    self.comparison.df2_unq_rows.to_excel(writer, sheet_name='tgt_unique_rows', index=False)
            logger.info(f"Comparison report saved at: {output_path}")
        except Exception as e:
            logger.error(f"Failed to save report: {e}")

