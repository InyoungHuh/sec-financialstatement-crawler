import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine
from src import data_writer
from src import db_connect
from src import config

class DataCrawler():
    # base_url = r'http://www.sec.gov/Archives/edgar/data'

    def __init__(self, company):
        self.company = company
        self.base_url = r'http://www.sec.gov/Archives/edgar/data'
        self.report_type = ['10-k', '10-q']

        self.dbwriter = data_writer.DataWriter()
        self.dbconn = db_connect.DatabaseConnection()
        self.config = config.Config()

    # TODO : implement crawling CIK number by company name
    def get_cik_number(company):
        return '/0000002488/'

    def get_filing_dict(self, cik_num, start_date, last_date):

        filings_url = self.base_url + cik_num + "/index.json"
        content = requests.get(filings_url)
        decoded_content = content.json()

        # get FilingSummary.xml
        filing_dict = {}
        for filing in decoded_content['directory']['item']:
            if start_date < filing['last-modified'] <= last_date:
                filing_num = filing['name']

                filing_url = self.base_url + cik_num + filing_num + "/index.json"

                content = requests.get(filing_url)
                document_content = content.json()
                for document in document_content['directory']['item']:
                    if document['name'] == 'FilingSummary.xml':
                        document_url = self.base_url + cik_num + filing_num + '/' + document['name']
                        filing_dict[filing['last-modified']] = document_url
                        break;

        # filing_dict = {last-modified : document_url}
        return filing_dict

    # statement_lists : lists what I want e.g) balance sheets, statements_of_operations
    # get url info for each reporing list
    def get_reporting_statement_url(self, statements_lists, filing_dict):
        statements_url = []

        for timestamp, filing_summary_url in filing_dict.items():
            content = requests.get(filing_summary_url).content
            soup = BeautifulSoup(content, 'lxml')
            reports = soup.find('myreports')

            # reports_collections for gathering url/location info for statements lists in FilingSummary.xml
            reports_collection = []
            for report in reports.find_all('report')[:-1]:
                report_dict = {}
                report_dict['name_short'] = report.shortname.text
                report_dict['name_long'] = report.longname.text
                if report.htmlfilename is not None:
                    report_dict['url'] = report.htmlfilename.text
                else:
                    report_dict['url'] = report.xmlfilename.text
                reports_collection.append(report_dict)

            for report_dict in reports_collection:
                if report_dict['name_short'].lower() in statements_lists:
                    url_info = {}
                    url_info['statement'] = report_dict['name_short'].lower()
                    url_info['last_url'] = report_dict['url']
                    url_info['full_url'] = filing_summary_url.replace('FilingSummary.xml', '') + report_dict['url']
                    statements_url.append([timestamp, url_info])

        return statements_url

    # Only get url['statement'] is document entry
    def get_statement_data(self, statements_url, statement_type):
        statements_data = []

        statement_data = {'headers': [], 'sections': [], 'data': []}

        full_url = statements_url['full_url']

        content = requests.get(full_url).content
        report_soup = BeautifulSoup(content, 'lxml')

        try:
            for index, row in enumerate(report_soup.table.find_all('tr')):
                cols = row.find_all('td')
                if len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0:
                    reg_row = [ele.text.strip().lower() for ele in cols]
                    non_reg_row = [ele if '[' not in ele and ']' not in ele else None for ele in reg_row]
                    statement_data['data'].append(non_reg_row)

                elif len(row.find_all('th')) == 0 and len(row.find_all('strong')) != 0:
                    sec_row = cols[0].text.strip()
                    statement_data['sections'].append(sec_row)

                elif len(row.find_all('th')) != 0:
                    hed_row = [ele.text.strip() for ele in row.find_all('th')]
                    statement_data['headers'].append(hed_row)

                else:
                    print('We encountered an error.')

                statements_data.append(statement_data)

            # If statement is document_entry_reports, extract type and end_date in document entry reports
            if statement_type == 'cover':
                for data in statements_data[0]['data']:
                    if data[0] == 'document type':
                        doc_type = data[1]
                    if data[0] == 'document period end date':
                        end_date = datetime.strptime(data[1], '%b. %d, %Y')
                return doc_type, end_date
            # Otherwise, extracting table data
            else:
                return statement_data
        except:
            print('No table for this url : ' + full_url)

    def get_statement_information(self, statements_url, statement_type):
        statements_info = {}

        for statement_url in statements_url:
            info = {}
            timestamp = statement_url[0]
            url_info = statement_url[1]

            info['timestamp'] = timestamp
            info['url'] = url_info['full_url']

            if statement_type == 'cover':
                info['type'], info['endtime'] = self.get_statement_data(url_info, statement_type)
            else:
                info.update(self.get_statement_data(url_info, statement_type))

            statements_info.update({timestamp: info})

        # if type is 'cover', {timestamp, url, type, endtime}
        # otherwise, {timestamp, url, data}
        return statements_info

    def get_quarterly_reports(self, statements_cover_info, statements_balance_sheets_info,
                              statements_of_operations_info):
        quarterly_reports = []
        for timestamp, cover_info in statements_cover_info.items():
            quarterly_report = {}
            # only 10-K and 10-Q
            if cover_info['type'] in self.report_type:
                quarterly_report['type'] = cover_info['type']
                quarterly_report['endtime'] = cover_info['endtime']
                quarterly_report['balance sheets'] = statements_balance_sheets_info.get(timestamp)['data']
                quarterly_report['operation_info'] = statements_of_operations_info.get(timestamp)['data']
                quarterly_reports.append(quarterly_report)

        return quarterly_reports

    def prepare_to_data_for_SQL(self, quarterly_reports, form):
        for idx in range(len(quarterly_reports)):
            test_df = pd.DataFrame(quarterly_reports[idx][form])

            # convert to pandas dataframes
            test_df1 = test_df.replace('[\$,)\[\]]', '', regex=True).replace('[(]', '-', regex=True).replace('', 'NaN',
                                                                                                             regex=True)
            test_df1.index = test_df1[0]
            test_df1.index.name = 'Category'
            test_df1 = test_df1.drop(0, axis=1)

            test_df1 = test_df1.apply(pd.to_numeric, errors='coerce').astype(float)
            #print(test_df1)

            index_name = self.config.config("STATEMENT_OF_OPERATION_INDEX")
            column_idx = 1 if pd.isnull(test_df1.iloc[0, 0]) else 0
            df = test_df1[[s in index_name for s in test_df1.index]].T.iloc[column_idx, :].astype(float)

            month = self.get_quarter(quarterly_reports[idx]['endtime'].month)
            final_df_headers = self.config.config("STATEMENT_OF_OPERATION_HEADER")

            df = df.rename(final_df_headers)
            df['Form'] = quarterly_reports[idx]['type']
            df['Year'] = quarterly_reports[idx]['endtime'].year
            df['Quarter'] = month
            print(df)
            table_name = self.config.config("POSTGRES_DATABASE_TABLES")
            sql, params = self.dbwriter.insert_statement_operation_SQL(table_name,df)
            self.dbconn.execute(sql, params)
            self.dbconn.commit()
            print("end")

    def get_quarter(self, month):
        if month in [1, 2, 3]:
            quarter = 1
        elif month in [4, 5, 6]:
            quarter = 2
        elif month in [7, 8, 9]:
            quarter = 3
        else:
            quarter = 4
        return quarter


if __name__ == "__main__":
    print("start")
    company = "AMD"
    start_date = '2018-01-01'
    last_date = '2021-01-01'

    # title of statment varies from company to company. Should check first for FilingSummary.xml
    # should be lower case
    statement_list = {'cover': [r"document and entity information", "cover page"],
                      'balance_sheets': [r"consolidated balance sheets"
                          , "consolidated balance sheets (unaudited)"
                          , r"condensed consolidated balance sheets"
                          , r"condensed consolidated balance sheets (unaudited)"],
                      'statements_of_operations': [r"consolidated statements of operations"
                          , "consolidated statements of operations (unaudited)"
                          , r"condensed consolidated statements of operations"
                          , "condensed consolidated statements of operations (unaudited)"]}

    crawler = DataCrawler(company)
    cik_num = crawler.get_cik_number()

    # get FilingSummary.xml url
    filing_dict = crawler.get_filing_dict(cik_num, start_date, last_date)

    # get urls for cover page, balance sheets and statements_of_operations
    statements_cover_url = crawler.get_reporting_statement_url(statement_list['cover'], filing_dict)
    statements_balance_sheets_url = crawler.get_reporting_statement_url(statement_list['balance_sheets'], filing_dict)
    statements_of_operations_url = crawler.get_reporting_statement_url(statement_list['statements_of_operations'],
                                                                       filing_dict)
    # get table data for each different type of statements
    statements_cover_info = crawler.get_statement_information(statements_cover_url, 'cover')
    statements_balance_sheets_info = crawler.get_statement_information(statements_balance_sheets_url, 'balance_sheets')
    statements_operations_info = crawler.get_statement_information(statements_of_operations_url,
                                                                   'statements_of_operations')

    # consolidate all statements
    quarterly_reports = crawler.get_quarterly_reports(statements_cover_info, statements_balance_sheets_info,
                                                      statements_operations_info)

    crawler.prepare_to_data_for_SQL(quarterly_reports, 'operation_info')
