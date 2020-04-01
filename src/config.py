class Config:
    __conf = {
        "username": "",
        "password": "",
        "MYSQL_PORT": 3306,
        "MYSQL_DATABASE": 'mydb',
        "POSTGRES_DATABASE_TABLES": 'amd_statement_of_operation',
        "STATEMENT_OF_OPERATION_INDEX": ['net revenue', 'cost of sales', 'gross profit', 'gross margin', 'research and development',
                          'operating income', 'operating loss','operating income -loss', 'net income -loss', 'net income', 'net income'],
        "STATEMENT_OF_OPERATION_HEADER": {'net revenue': 'NetRevenue',
                                          'cost of sales': 'CostOfSales',
                                          'gross profit': 'GrossMargin',
                                          'gross margin': 'GrossMargin',
                                          'research and development': 'ResearchAndDevelopment',
                                          'operating income': 'OperatingCost',
                                          'operating loss': 'OperatingCost',
                                          'operating income -loss': 'OperatingCost',
                                          'net income': 'NetTotal',
                                          'net loss': 'NetTotal',
                                          'net income -loss': 'NetTotal'}
    }
    __setters = ["username", "password"]

    @staticmethod
    def config(name):
        return Config.__conf[name]

    @staticmethod
    def set(name, value):
        if name in Config.__setters:
            Config.__conf[name] = value
        else:
            raise NameError("Name not accepted in set() method")
