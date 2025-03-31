import pandas as pd
import datetime


class ReconReportGenerator :
    def __init__(self, comparison, source_data, target_data, config):
        self.comparison = comparison
        self.source_data = source_data
        self.target_data = target_data
        self.config = config

    def recon_report(self):
        df_col_stats = self.comparison.column_stats
        src_row_count = self.source_data.shape[0]
        src_col_count = len(self.source_data.columns)
        tgt_row_count = self.target_data.shape[0]
        tgt_col_count = len(self.target_data.columns)

        common_rows_count = self.comparison.intersect_rows.shape[0]
        rows_in_src_only = self.comparison.df1_unq_rows.shape[0]
        rows_in_tgt_only = self.comparison.df2_unq_rows.shape[0]
        rows_having_mismatch = self.comparison.all_mismatch().shape[0]
        row_having_no_mismatch = common_rows_count - rows_having_mismatch

        common_cols_count = len(self.comparison.intersect_columns())
        cols_in_src_only_count = len(self.comparison.df1_unq_columns())
        cols_in_tgt_only = len(self.comparison.df2_unq_columns())

        cols_having_no_mismatch = sum(1 for col in df_col_stats if col['all_match'])
        cols_having_mismatch = sum(1 for col in df_col_stats if not col['all_match'])

        summary_chart_data = [src_row_count, src_col_count, tgt_row_count, tgt_col_count]
        row_summary_data = [common_rows_count, rows_in_src_only, rows_in_tgt_only, row_having_no_mismatch,
                            rows_having_mismatch]
        col_summary_data = [common_cols_count, cols_in_src_only_count, cols_in_tgt_only, cols_having_no_mismatch,
                            cols_having_mismatch]

        matched_keys = str(self.comparison.join_columns)
        absolute_tole = str(self.comparison.abs_tol)
        relative_tole = str(self.comparison.rel_tol)

        dupe_row_in_src_count = 0
        dupe_row_in_tgt_count = 0

        comparison_keys_lower = [key.lower() for key in self.config['comparison_keys']]

        if not self.comparison.df1_unq_rows.empty:
            dupe_row_in_src = pd.merge(self.comparison.df1_unq_rows, self.comparison.intersect_rows,
                                       on=comparison_keys_lower, how='inner')
            dupe_row_in_src_count = dupe_row_in_src.shape[0]
        if not self.comparison.df2_unq_rows.empty:
            dupe_row_in_tgt = pd.merge(self.comparison.df2_unq_rows, self.comparison.intersect_rows,
                                       on=comparison_keys_lower, how='inner')
            dupe_row_in_tgt_count = dupe_row_in_tgt.shape[0]

        spaces_ignored = self.comparison.ignore_spaces
        df_col_with_uneq_values_types = pd.DataFrame.from_dict(self.comparison.column_stats)
        all_mismatch = self.comparison.all_mismatch()

        styled_df = all_mismatch.style.apply(self.highlight_diff, axis=1)
        styled_html = styled_df.hide(axis=0)._repr_html_()

        # Generating report file name
        now = datetime.datetime.now()
        date_str = now.strftime("%Y-%m-%d")  # Format: YYYY-MM-DD
        time_str = now.strftime("%H_%M_%S")  # Format: HH_MM_SS
        datetime_str = f"_{date_str}_{time_str}"
        recon_report = f"./resources/recon_reports/recon_report_{self.config['source_name']}_{self.config['target_name']}{datetime_str}.html"

        with open('./resources/report_tmpl/recon_report_tmpl.html', 'r') as input_report_file, open(recon_report,
                                                                                       'w') as output_report_file:
            html_report = input_report_file.read()
            html_report = html_report.replace("#absolute_tole#", absolute_tole)
            html_report = html_report.replace("#relative_tole#", relative_tole)
            html_report = html_report.replace("#matched_keys#", matched_keys)
            html_report = html_report.replace("#Columns with un-eq values / types#",
                                              df_col_with_uneq_values_types.to_html())
            html_report = html_report.replace("#Rows with un-eq values#", styled_html)
            html_report = html_report.replace("#Rows Only In Src#", self.comparison.df1_unq_rows.to_html())
            html_report = html_report.replace("#Rows Only In Tgt#", self.comparison.df2_unq_rows.to_html())
            html_report = html_report.replace("#Summary Chart#", str(summary_chart_data))
            html_report = html_report.replace("#Row Summary#", str(row_summary_data))
            html_report = html_report.replace("#Column Summary#", str(col_summary_data))
            html_report = html_report.replace("#duplicate_flag_src#", 'Yes' if dupe_row_in_src_count > 0 else 'No')
            html_report = html_report.replace("#duplicate_flag_tgt#", 'Yes' if dupe_row_in_tgt_count > 0 else 'No')
            html_report = html_report.replace("#spaces_ignored#", str(spaces_ignored))
            output_report_file.write(html_report)

    def highlight_diff(self, row):
        styles = [''] * len(row)
        for i in range(1, len(row), 2):
            if row.iloc[i] != row.iloc[i + 1]:
                styles[i] = 'background-color: #FF6347'
                styles[i + 1] = 'background-color: #FF6347'
        return styles
