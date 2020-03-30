
# Web crawler for financial statement data from US securities and exchange
The ultimate goal of this project is providing 'balance sheets' and 'statements of operation' by scaping in U.S securities and exchange commission. Saved them in postgre sql

## Installation

```bash
pip3 install requests
pip3 install beautifulsoup4
```

1. get filingSummary.xml url
![](image/filing_dict.png)

2. statement of operations dictionary
![](image/statement_of_operations_dict.png)
![](image/statement_of_operations_df.png)

3. quarterly statement
![](image/quarterly_statement.png)