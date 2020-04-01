class DataWriter():

    def insert_statement_operation_SQL(self, table_name, df):
        insert_query = """ INSERT INTO """ + table_name \
                       + """(form, year, quarter, net_revenue, cost_of_sales, gross_margin, r_and_d, 
                                operating_cost, net_total) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        record_to_insert = (
        df['Form'], df['Year'], df['Quarter'], df['NetRevenue'], df['CostOfSales'], df['GrossMargin'],
        df['ResearchAndDevelopment'], df['OperatingCost'], df['NetTotal'])
        # print sql

        return insert_query, record_to_insert
