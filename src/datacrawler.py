import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime


class DataCrawler():
    # base_url = r'http://www.sec.gov/Archives/edgar/data'

    def __init__(self, company):
        self.company = company
        self.base_url = r'http://www.sec.gov/Archives/edgar/data'
        self.report_type = ['10-K', '10-Q']

    def get_cik_number(self):
        return '/320193/'

    def get_filing_dict(self, cik_num, last_date):

        filings_url = self.base_url + cik_num + "/index.json"

        content = requests.get(filings_url)
        decoded_content = content.json()
        decoded_content

        # get FilingSummary.xml
        filing_dict = {}
        for filing in decoded_content['directory']['item']:
            if filing['last-modified'] > last_date:
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
                report_dict = {'name_short': report.shortname.text, 'name_long': report.longname.text}
                if report.htmlfilename is not None:
                    report_dict['url'] = report.htmlfilename.text
                else:
                    report_dict['url'] = report.xmlfilename.text
                reports_collection.append(report_dict)

            for report_dict in reports_collection:
                for statement_list in statements_lists:
                    if statement_list in report_dict['name_short'].lower():
                        url_info = {'statement': report_dict['name_short'].lower(), 'last_url': report_dict['url'],
                                    'full_url': filing_summary_url.replace('FilingSummary.xml', '') + report_dict[
                                        'url']}
                        statements_url.append([timestamp, url_info])

        return statements_url

    # Only get url['statement'] is document entry
    def get_statement_data(self, statements_url, statement_type):
        statements_data = []

        statement_data = {'headers': [], 'sections': [], 'data': []}

        full_url = statements_url['full_url']

        content = requests.get(full_url).content
        report_soup = BeautifulSoup(content, 'html')

        try:
            for index, row in enumerate(report_soup.table.find_all('tr')):
                cols = row.find_all('td')
                if len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0:
                    reg_row = [ele.text.strip() for ele in cols]
                    # print(reg_row)
                    statement_data['data'].append(reg_row)

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
                    if data[0] == 'Document Type':
                        doc_type = data[1]
                    if data[0] == 'Document Period End Date':
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

    def get_quarterly_report(self, statements_cover_info, statements_balance_sheets_info,
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


if __name__ == "__main__":
    company = "AMD"
    last_date = '2017-01-01'

    # title of statment varies from company to company. Should check first for FilingSummary.xml
    # should be lower case
    statement_list = {'cover': [r"document and entity information", "cover page"],
                      'balance_sheets': [r"consolidated balance sheets"],
                      'statements_of_operations': [r"consolidated statements of operations"]}

    crawler = DataCrawler(company)
    cik_num = crawler.get_cik_number()

    # get FilingSummary.xml url
    filing_dict = crawler.get_filing_dict(cik_num, last_date)
    print(filing_dict)

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
