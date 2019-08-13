import numpy as np
import pandas as pd
import os
import datetime

class Mapper:
    def __init__(self):
        # Column names for Xero csv file
        self.columns = [
            '*ContactName', 
            '*InvoiceNumber', 
            '*InvoiceDate', 
            '*DueDate', 
            'InventoryItemCode', 
            '*Description', 
            '*Quantity',
            '*UnitAmount',
            '*AccountCode',
            '*TaxType'
        ]

        self.lines = []
        self.csv_path = 'data/csv/upload.csv'
        self.reports_path = 'data/reports/'

    def add_line(self, description, qty, unit_amount, item=''):
        contact_name = 'Amazon'
        account_code = self.get_code(description)
        tax_type = self.get_tax_type(description)

        # returns cannont use the itemcode
        if item != '' and unit_amount < 0:
            description = item + ' returns'
            item = ''
    
        line = [
            contact_name,
            self.invoice_number,
            self.date,
            self.date,
            item,
            description,
            qty,
            unit_amount,
            account_code,
            tax_type
        ]
    
        return line

    def get_tax_type(self, description):
        if description == '':
            return
        tax = {
            'sales': 'Tax on Sales',
            'purchases': 'Tax on Purchases',
            'exe': 'Tax Exempt'
        }
    
        mapping = {
            'transaction details': tax['exe'],
            'amazon fees': tax['purchases'],
            'promo rebates': tax['exe'],
            'other': tax['exe'],
            'product charges': tax['sales'],
            'fba inventory reimbursement - customer return': tax['sales']
        }
        return mapping[description]

    def get_code(self, description):
        if description == '':
            return
        mapping = {
            'transaction details': 6000,
            'amazon fees': 6150,
            'promo rebates': 4500,
            'other': 2500,
            'product charges': 4000,
            'fba inventory reimbursement - customer return':'a2xreimb'  
        }
    
        return mapping[description]

    def preprocess(self, df):
    
        # change nan to empty string
        df = df.replace(np.nan, '', regex=True)
    
        # convert text amount to float
        df['Amount'] = df['Amount'].apply(lambda x: x.replace('$', ''))
        df['Quantity'] = df['Quantity'].replace(r'^\s*$', 1, regex=True)
        df = df.astype({'Amount': 'float64'})
        df = df.astype({'Quantity': 'float64'})
    
        # clean descriptions
        df['Payment Detail'] = df['Payment Detail'].apply(lambda x: " ".join(x.replace('&', '').split()).lower())
        df['Payment Type'] = df['Payment Type'].apply(lambda x: " ".join(x.split()).lower())

        # remove tax, merchant doesnt see it
        df = df[df['Payment Detail'] != 'product tax']
        df = df[df['Payment Detail'] != 'shipping tax']

        # fix quantities and amounts
        df['amznAmount'] = df['Amount']
        df['Amount'] = df['Amount'] / df['Quantity'] 

        return df

    def process_report(self, df, invoice_number):
        self.invoice_number = invoice_number
        # Group and sum all fees that aren't items
        payment_detail = df[df['Payment Type'] != 'product charges'].groupby(['Payment Type'])['Amount'].sum()

        # Group all item lines
        items = df[df['Payment Type'] == 'product charges'].groupby(['SKU', 'Amount', 'Payment Type'])['Quantity'].sum()

        # Create csv lines for non-items
        for index, value in payment_detail.items():
            self.lines.append(self.add_line(description=index, qty=1, unit_amount=value))

        # Create csv lines for items
        for index, value in items.items():
            self.lines.append(self.add_line(description=index[2], qty=value, unit_amount=index[1], item=index[0]))
       
    def create_csv(self):
            csv_df = pd.DataFrame(self.lines, columns=list(self.columns))

            csv_df['Amount'] = csv_df['*UnitAmount'] * csv_df['*Quantity']
            #print(csv_df.head(50))
            print(csv_df.groupby('*InvoiceNumber')['Amount'].sum())
            csv_df.drop('Amount', axis=1, inplace=True)
            csv_df.to_csv(self.csv_path, index=False)

    def load_report(self, path):
        # Skip top couple of rows because its a summary
        df = pd.read_csv(path, header=2, sep='\t')     
        df = self.preprocess(df)
        self.date = datetime.datetime.strptime(df['Date'][0], '%b %d, %Y').strftime('%m/%d/%y')
        return df

    def map_reports(self):
        files = os.listdir(self.reports_path)

        for filename in files:
            # reports should be saved as the INV-0000.txt number you want on the CSV
            invoice_number = filename[:-4]
            self.df = self.load_report(self.reports_path+filename)
            self.process_report(self.df, invoice_number)
            self.report_stats(self.df)

        self.create_csv()

    def report_stats(self, df):
        print(df.groupby(['Payment Detail', 'Payment Type'])['amznAmount'].sum())
        print('Total: ')
        print(df['amznAmount'].sum())
        print('\n')


mapper = Mapper()
mapper.map_reports()

