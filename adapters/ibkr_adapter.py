import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, timedelta
from ib_insync import IB, Stock, util

# Configuration constants
IB_HOST = '127.0.0.1'
IB_PORTS = [4001, 4002, 7496, 7497]
IB_CLIENT_ID = 10 

class IBKRFinancialAdapter:
    # Financial Statement COA Codes Mapping
    COA_MAP = {
        'INC': { 
            'REVE': 'Total Revenue', 'COGS': 'Cost Of Revenue', 'SGA': 'Selling General And Administration',
            'RDEP': 'Research And Development', 'OPER': 'Operating Income', 'EBIT': 'EBIT',
            'INTA': 'Interest Expense', 'TAXE': 'Tax Provision', 'NINC': 'Net Income',
            'NICS': 'Net Income Common Stockholders', 'EPSB': 'Basic EPS', 'EPSD': 'Diluted EPS'
        },
        'BAL': {
            'ATOT': 'Total Assets', 'LTOT': 'Total Liabilities', 'QTOT': 'Total Equity',
            'SCDE': 'Common Stock Equity', 'CASH': 'Cash And Cash Equivalents', 'ACRE': 'Net Receivables',
            'INVE': 'Inventory', 'PPNE': 'Property Plant Equipment', 'GODW': 'Goodwill',
            'LTDN': 'Long Term Debt', 'APAY': 'Accounts Payable'
        },
        'CAS': {
            'OTLO': 'Cash Flow From Continuing Operating Activities',
            'ITLO': 'Cash Flow From Continuing Investing Activities',
            'FTLO': 'Cash Flow From Continuing Financing Activities',
            'CAPX': 'Capital Expenditure', 'DVDP': 'Dividends Paid'
        }
    }

    @staticmethod
    def parse_financial_statements(xml_data: str):
        if not xml_data: return None
        try: root = ET.fromstring(xml_data)
        except: return None
        results = {'annual': {'income': pd.DataFrame(), 'balance': pd.DataFrame(), 'cash': pd.DataFrame()}, 
                   'quarterly': {'income': pd.DataFrame(), 'balance': pd.DataFrame(), 'cash': pd.DataFrame()}}
        data_map = {'INC': {'ANN': {}, 'QTR': {}}, 'BAL': {'ANN': {}, 'QTR': {}}, 'CAS': {'ANN': {}, 'QTR': {}}}
        for statement in root.findall('.//Statement'):
            st_type = statement.get('Type')
            if st_type not in data_map: continue
            mapping = IBKRFinancialAdapter.COA_MAP.get(st_type, {})
            for fp in statement.findall('.//FP'):
                p_type = fp.get('PeriodType')
                if p_type not in ['ANN', 'QTR']: continue
                date_str = fp.get('EndDate')
                if not date_str: continue
                dt = pd.to_datetime(date_str)
                if dt not in data_map[st_type][p_type]: data_map[st_type][p_type][dt] = {}
                for item in fp.findall('.//lineItem'):
                    coa = item.get('coaCode')
                    label = mapping.get(coa)
                    if label:
                        try: data_map[st_type][p_type][dt][label] = float(item.text)
                        except: pass
        mapping_keys = {'INC': 'income', 'BAL': 'balance', 'CAS': 'cash'}
        period_keys = {'ANN': 'annual', 'QTR': 'quarterly'}
        for st_type, p_maps in data_map.items():
            st_key = mapping_keys[st_type]
            for p_type, d_map in p_maps.items():
                p_key = period_keys[p_type]
                if d_map:
                    df = pd.DataFrame(d_map)
                    df = df[sorted(df.columns, reverse=True)]
                    results[p_key][st_key] = df
        return results

    @staticmethod
    def parse_snapshot(xml_data: str):
        if not xml_data: return {}
        try: root = ET.fromstring(xml_data)
        except: return {}
        info = {}
        ratio_map = {
            'APEVY': 'trailingPE', 'PRICE2BK': 'priceToBook', 'TTMROETIG': 'returnOnEquity',
            'TTMOPMGN': 'operatingMargins', 'CURRATIO': 'currentRatio', 'TDE2EQ': 'debtToEquity',
            'MKTCAP': 'marketCap', 'NHIG': 'fiftyTwoWeekHigh', 'NLOW': 'fiftyTwoWeekLow',
            'DIVYMTTM': 'dividendYield', 'TTMREVSCH': 'revenueGrowth', 'TTMEPSSCH': 'earningsGrowth'
        }
        for ratio in root.findall('.//Ratio'):
            field = ratio.get('FieldName')
            if field in ratio_map:
                try:
                    val = float(ratio.text)
                    if field == 'TDE2EQ' and val < 10: val *= 100
                    info[ratio_map[field]] = val
                except: pass
        for co_id in root.findall('.//CoID'):
            if co_id.get('IDType') == 'CompanyName': info['longName'] = co_id.text
        industry = root.find('.//Industry')
        if industry is not None:
            info['industry'] = industry.text
            info['sector'] = industry.get('Sector')
        return info


class IBKRConnector:
    def __init__(self, host=IB_HOST, ports=IB_PORTS, client_id=IB_CLIENT_ID):
        self.host = host
        self.ports = ports
        self.client_id = client_id
        self.ib = IB()
        self._connected = False

    async def connect(self):
        if self._connected: return True
        for port in self.ports:
            try:
                await self.ib.connectAsync(self.host, port, clientId=self.client_id)
                self._connected = True
                return True
            except: continue
        return False

    async def get_fundamental_data(self, symbol, report_type):
        if not self._connected and not await self.connect(): return None
        try:
            if '.' in symbol: market, ticker_part = symbol.split('.', 1)
            else: market, ticker_part = 'US', symbol
            
            ticker_part = ticker_part.replace('.', ' ')
            if market == 'US': contract = Stock(ticker_part, 'SMART', 'USD')
            elif market == 'HK': contract = Stock(ticker_part.lstrip('0'), 'SEHK', 'HKD')
            else: return None
            
            await self.ib.qualifyContractsAsync(contract)
            data = await self.ib.reqFundamentalDataAsync(contract, report_type)
            return data
        except: return None

    async def get_market_price(self, symbol):
        if not self._connected and not await self.connect(): return None
        try:
            if '.' in symbol: market, ticker_part = symbol.split('.', 1)
            else: market, ticker_part = 'US', symbol
            ticker_part = ticker_part.replace('.', ' ')
            if market == 'US': contract = Stock(ticker_part, 'SMART', 'USD')
            elif market == 'HK': contract = Stock(ticker_part.lstrip('0'), 'SEHK', 'HKD')
            else: return None
            await self.ib.qualifyContractsAsync(contract)
            [ticker] = await self.ib.reqTickersAsync(contract)
            return ticker.marketPrice()
        except: return None

    async def fetch_history(self, symbol, duration='1 Y'):
        if not self._connected and not await self.connect(): return None
        try:
            if '.' in symbol: market, ticker_part = symbol.split('.', 1)
            else: market, ticker_part = 'US', symbol
            ticker_part = ticker_part.replace('.', ' ')
            if market == 'US': contract = Stock(ticker_part, 'SMART', 'USD')
            elif market == 'HK': contract = Stock(ticker_part.lstrip('0'), 'SEHK', 'HKD')
            else: return None
            await self.ib.qualifyContractsAsync(contract)
            bars = await self.ib.reqHistoricalDataAsync(contract, '', duration, '1 day', 'TRADES', True, 1)
            df = util.df(bars)
            if df is not None:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            return df
        except: return None


class IBKRTickerAdapter:
    def __init__(self, symbol, connector):
        self.symbol = symbol
        self.connector = connector
        self.info = {}
        self.income_stmt = pd.DataFrame()
        self.balance_sheet = pd.DataFrame()
        self.cashflow = pd.DataFrame()
        self.quarterly_income_stmt = pd.DataFrame()
        self.quarterly_balance_sheet = pd.DataFrame()
        self.quarterly_cashflow = pd.DataFrame()
        self._initialized = False

    async def init(self):
        if self._initialized: return self
        xml = await self.connector.get_fundamental_data(self.symbol, 'ReportsFinStatements')
        if xml:
            res = IBKRFinancialAdapter.parse_financial_statements(xml)
            if res:
                self.income_stmt = res['annual'].get('income', pd.DataFrame())
                self.balance_sheet = res['annual'].get('balance', pd.DataFrame())
                self.cashflow = res['annual'].get('cash', pd.DataFrame())
                self.quarterly_income_stmt = res['quarterly'].get('income', pd.DataFrame())
                self.quarterly_balance_sheet = res['quarterly'].get('balance', pd.DataFrame())
                self.quarterly_cashflow = res['quarterly'].get('cash', pd.DataFrame())
        
        xml_snap = await self.connector.get_fundamental_data(self.symbol, 'ReportSnapshot')
        if xml_snap: self.info = IBKRFinancialAdapter.parse_snapshot(xml_snap)
        
        price = await self.connector.get_market_price(self.symbol)
        if price: self.info['currentPrice'] = price
        
        self._initialized = True
        return self

    async def history(self, period='1y', interval='1d'):
        d_map = {'1mo': '1 M', '3mo': '3 M', '6mo': '6 M', '1y': '1 Y', '2y': '2 Y', '5y': '5 Y', 'max': '10 Y'}
        return await self.connector.fetch_history(self.symbol, d_map.get(period, '1 Y'))
