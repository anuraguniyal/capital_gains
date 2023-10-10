import csv
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import queue

class StockOption:
    def __init__(self, symbol, _type, strike, date):
        self.symbol = symbol
        self.strike = strike
        assert _type in ['call', 'put']
        self.type = _type
        self.date = date

    def __str__(self):
        """also act as option id"""
        return f"{self.symbol} {self.date} {self.type}@{self.strike}"

class StockTrade:
    SYMBOL_ALIAS = {
        "FB": "META"
    }
    SECURITY_TYPES = ['stock', 'option']
    def __init__(self, row):
        self.row = row # original data
        # cusip or some unique thing
        # i couldn't get some standard cusip data and for many csv
        # it is missing, so just lets stick with symbol
        #self.id = None # cusip or some unique thing
        self._symbol = None
        self._security_type = None
        self.date = None
        self.quantity = None # -ive means sell
        # we do not use this, but for etrade we set it
        # and trade_type confusion is set for option expiry
        # where we don't know if sell/buy has expired
        self.trade_type = None
        self._price = None
        self.option = None
        self.fake = False # added to close some unclosed options

    def copy(self):
        t = StockTrade(self.row)
        t.symbol = self.symbol
        t.security_type = self.security_type
        t.date = self.date
        t.quantity = self.quantity
        t.price = self.price
        t.option = self.option
        return t

    @property
    def price(self):
        return self._price
    @price.setter
    def price(self, value):
        if value < 0:
            value = -value
        self._price = value

    @property
    def amount(self):
        return self.quantity*self.price

    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, value):
        value = value.upper()
        if value in self.SYMBOL_ALIAS:
            value = self.SYMBOL_ALIAS[value]
        self._symbol = value

    @property
    def security_type(self):
        return self._security_type

    @security_type.setter
    def security_type(self, value):
        if value not in self.SECURITY_TYPES:
            raise ValueError(f"{value} should be one on of {self.SECURITY_TYPES}")
        self._security_type = value

    def match(self, trade):
        """
        match this trade with another
        return remaing trade
        e.g. 10 by matched with 5 sell, with return 5 buy trade + TradePair
        """
        remain_q = self.quantity + trade.quantity
        if remain_q != 0:
            # remain will be remove from same sign quntity
            if remain_q*self.quantity > 0:
                remain_trade = self.copy()
                t1 = self.copy()
                t1.quantity = self.quantity - remain_q
                t2 = trade
            else:
                remain_trade = trade.copy()
                t1 = self
                t2 = trade.copy()
                t2.quantity = trade.quantity - remain_q
            remain_trade.quantity = remain_q
        else:
            remain_trade = None
            t1 = self
            t2 = trade

        return remain_trade, TradePair(t1, t2)

    def __str__(self):
        if self.security_type == 'option':
            op = str(self.option)
        else:
            op = ''
        return f"{self.symbol} {op} {self.date} {self.quantity}@{self.price:.2f}={self.amount:.2f}"

    __repr__ = __str__

class TradePair:
    def __init__(self, t1, t2):
        assert t1.quantity*t2.quantity < 0
        assert t1.symbol == t2.symbol
        assert t1.security_type == t2.security_type

        # sort by dates
        l = [t1, t2]
        l.sort(key=lambda t:[t.date, -t.quantity])
        self.t1, self.t2 = l
        self.symbol = self.t1.symbol
        self.security_type = self.t1.security_type
        self.quantity = self.t1.quantity
        self.date1 = self.t1.date
        self.date2 = self.t2.date
        self.profit = -(t1.amount + t2.amount)
        self.fake = self.t1.fake or self.t2.fake
        # check if short sale
        self.short = False
        if self.date2 < self.date1 + timedelta(days=366):
            self.short = True

    def __str__(self):
        fk=''
        if self.fake:
            fk += ' !'
        if self.t1.option is not None:
            sym = f"{str(self.t1.option)+fk:<30}"
        else:
            sym = f"{self.symbol+fk:<5}"
        return f"{sym} {self.quantity:>4} {self.date1} {self.date2} {self.profit:.2f}"

class StockTrades:

    def __init__(self, symbol, close_options=True):
        self.symbol = symbol
        self.close_options = close_options
        self.stock_trades = []
        self.opt_trade_map = {}

    def add(self, trade):
        assert self.symbol == trade.symbol
        if trade.security_type == 'stock':
            self.stock_trades.append(trade)
        else:
            opt_id = str(trade.option)
            if opt_id not in self.opt_trade_map:
                self.opt_trade_map[opt_id] = []
            self.opt_trade_map[opt_id].append(trade)

    def option_pairs(self):
        pairs = []
        for opt_id, trades in self.opt_trade_map.items():
            pairs += self.pair_trades(trades)
        return pairs

    def stock_pairs(self):
        return self.pair_trades(self.stock_trades)

    @classmethod
    def pair_trades(cls, trades):
        """
        create a buy and sell queue
        this way we can also handle shorted stock
        where we first sell and then buy
        """
        bq = queue.Queue()
        sq = queue.Queue()
        pairs = []
        for trade in trades:
            if trade.quantity > 0: # buy order
                # match with sell queue till its empty
                # or trade is matched
                while not sq.empty():
                    st = sq.get()
                    remain, pair = trade.match(st)
                    pairs.append(pair)
                    if remain is None:
                        trade = None
                        break
                    if remain.quantity < 0: #sell, put back remain
                        trade = None
                        sq.put(remain)
                        break
                    trade = remain # remain buy order
                if trade is not None:
                    bq.put(trade)
            else: # sell order
                # match with buy queue till its empty
                # or trade is matched
                while not bq.empty():
                    bt = bq.get()
                    remain, pair = trade.match(bt)
                    pairs.append(pair)
                    if remain is None:
                        trade = None
                        break
                    if remain.quantity > 0: #buy, put back remain
                        trade = None
                        bq.put(remain)
                        break
                    trade = remain # remain sell order
                if trade is not None:
                    sq.put(trade)

        if not sq.empty() or not bq.empty():
            raise ValueError(f"queue not empty, check for missing open/close txns,  initial trades {len(trades)}")
        return pairs

    def finish(self):
        """
        do final sorting and fix open sell options
        """
        self.stock_trades.sort(key=lambda t:t.date)

        # for etrade we have expired option but we don't know sell or buy
        for opt_id, trades in self.opt_trade_map.items():
            self.stock_trades.sort(key=lambda t:t.date)
            c_q = 0
            q = 0
            for trade in trades:
                if trade.trade_type == 'confusion':
                    c_q += trade.quantity
                else:
                    q += trade.quantity

            if q == 0 and c_q == 0:
                # all good
                continue

            if q + c_q == 0:
                # all good
                continue

            if q - c_q == 0:
                # lets flip confusion sign
                for trade in trades:
                    if trade.trade_type == 'confusion':
                        print("--flip-conufsion---")
                        trade.quantity = -trade.quantity

        # go thru options see if they are closed
        for opt_id, trades in self.opt_trade_map.items():
            q = 0
            for trade in trades:
                q += trade.quantity
            if q != 0:
                # doing this for chase which doesn't have expired options
                if self.close_options:
                    print(f"WARN: {opt_id} quantity {q} closing it")
                    trade = trades[0].copy()
                    trade.quantity = -q
                    trade.price = 0
                    trade.fake = True
                    trades.append(trade)
                else:
                    raise ValueError(f"{opt_id} quantity {q} open")

class StockCsvReader:
    CLOSE_OPTIONS = True
    def __init__(self, csv_files, filter_symbols=None):
        self.csv_files = csv_files
        self.trade_map = {}
        self.filter_symbols = filter_symbols
        self.load()

    def get_trade(self, row):
        # derived class so override it
        raise NotImplementedError

    def load(self):
        self.rows = []
        for csv_file in self.csv_files:
            with open(csv_file) as f:
                reader = csv.DictReader(f)
                self.rows += list(reader)

        for row in self.rows:
            trade = self.get_trade(row)
            if self.filter_symbols and trade.symbol not in self.filter_symbols:
                continue
            if trade.symbol not in self.trade_map:
                self.trade_map[trade.symbol] = StockTrades(trade.symbol, self.CLOSE_OPTIONS)
            self.trade_map[trade.symbol].add(trade)

        for trades in self.trade_map.values():
            trades.finish()

    def capital_gains(self):
        symbol_gains = {}
        for symbol, trades in self.trade_map.items():
            symbol_gains[symbol] = {
                    'short': 0,
                    'long':0,
                    'opt_short':0,
                    'opt_long':0
                    }
            pairs = trades.stock_pairs()
            if pairs:
                print(f"--- {symbol} ---")
                short_total = 0
                long_total = 0
                for pair in pairs:
                    print(pair)
                    if pair.short:
                        short_total += pair.profit
                    else:
                        long_total += pair.profit
                symbol_gains[symbol]['short'] = short_total
                symbol_gains[symbol]['long'] = long_total
            pairs = trades.option_pairs()
            if pairs:
                print(f"--- {symbol} options ---")
                short_total = 0
                long_total = 0
                for pair in pairs:
                    print(pair)
                    if pair.short:
                        short_total += pair.profit
                    else:
                        long_total += pair.profit
                symbol_gains[symbol]['opt_short'] = short_total
                symbol_gains[symbol]['opt_long'] = long_total

        short_total = 0
        long_total = 0
        for symbol, totals in symbol_gains.items():
            print(f"{symbol} short {totals['short']:.2f} long {totals['long']:.2f}")
            print(f"{symbol} options short {totals['opt_short']:.2f} long {totals['opt_long']:.2f}")
            short_total += totals['short']
            short_total += totals['opt_short']
            long_total += totals['long']
            long_total += totals['opt_long']

        print(f"Grand total --- short {short_total:.2f} long {long_total:.2f}")


class EtradeCsvReader(StockCsvReader):
    CLOSE_OPTIONS = False
    def get_trade(self, row):
        """{'Trade Date': '1/5/2022', 'Order Type': 'Buy To Close', 'Security': "FB JAN 07 '22 $347.50 CALL", 'Cusip': '', 'Transaction Description': "40     FB JAN 07 '22          $347.50 CALL(FB)              META PLATFORMS INC CL A       COVER SHORT", 'Quantity': '40', 'Executed Price': '0.08', 'Commission': '0.0000', 'Net Amount': '320.38'}"""
        trade = StockTrade(row)
        security = row['Security']
        if security.find('CALL') > 0 or security.find('PUT') > 0:
            trade.security_type = 'option'
            tokens = security.split()
            trade.symbol = tokens[0]
            option_date = tokens[1]+' '+tokens[2]+' '+tokens[3]
            option_date = datetime.strptime(option_date, "%b %d '%y").date()
            strike = Decimal(tokens[4][1:])
            option_type = tokens[5].lower()
            trade.option = StockOption(trade.symbol, option_type, strike, option_date)
        else:
            trade.security_type = 'stock'
            trade.symbol = security
        trade_type = row['Order Type'].lower()
        if trade_type == 'option expire':
            # we don't know if expired option is buy or sell
            # lets decided when closing options
            trade_type = 'confusion'
        if trade_type == 'option assignment':
            # conside option assignment as cost basis 0 option
            trade_type = 'buy'
        trade_type = trade_type.split()[0]
        if trade_type not in ['buy', 'sell', 'confusion']:
            raise ValueError(f"row unknown trade type {row}")
        trade.date = datetime.strptime(row['Trade Date'], '%m/%d/%Y').date()
        trade.quantity = int(row['Quantity'])
        if trade_type == 'sell':
            trade.quantity = -trade.quantity

        if trade_type == 'confusion':
            trade.quantity = -trade.quantity
        trade.trade_type = trade_type
        amount = Decimal(row['Net Amount']) # todo: may be store amount, as etrade/chase have amount
        trade.price = amount/trade.quantity
        return trade

class ChaseCsvReader(StockCsvReader):
    def get_trade(self, row):
        """
        any hadncrafted csv for extra txns should have
        Security Type, Type, Ticker, Description, Trade Date, Quantity, Price Local, Amount Local
        """
        trade = StockTrade(row)
        trade.security_type = row['Security Type'].lower()
        #trade.trade_type = row['Type'].lower()  # we just use quantity
        ticker = row['Ticker']
        # for option there is no ticker, so get it from desc
        if trade.security_type == 'option':
            # opt desc -> Description': 'CALL FB 01/21/22 330 META PLATFORMS INC CL
            tokens = row['Description'].split()
            option_type = tokens[0].lower()
            ticker = tokens[1]
            option_date = datetime.strptime(tokens[2], '%m/%d/%y').date()
            strike = Decimal(tokens[3])
            trade.symbol = ticker
            trade.option = StockOption(trade.symbol, option_type, strike, option_date)
        else:
            trade.symbol = ticker
        trade.date = datetime.strptime(row['Trade Date'], '%m/%d/%Y').date()
        trade.quantity = int(row['Quantity'])
        amount = Decimal(row['Amount Local'])
        trade.price = amount/trade.quantity
        return trade

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Calc some gainz.')
    parser.add_argument('csv_files', type=str, nargs='+',
                        help='csv transcation files')
    parser.add_argument('--csv-type', dest='csv_type', choices=['etrade', 'chase'],
                        default='etrade', help='type of csv files')
    parser.add_argument('--symbol', help='filter by symbol', nargs='*')

    args = parser.parse_args()
    if args.csv_type == 'chase':
        er = ChaseCsvReader(args.csv_files, args.symbol)
    elif args.csv_type == 'etrade':
        er = EtradeCsvReader(args.csv_files, args.symbol)
    # output csv file in format for https://github.com/nkouevda/capital-gains
    f = open("/tmp/1.csv", "w+")
    cw = csv.writer(f)
    cw.writerow(["date","symbol","name","shares","price","fee"])
    if False:
        for trades in er.trade_map.values():
            for opt_id, opt_trades in trades.opt_trade_map.items():
                for trade in opt_trades:
                    print(trade)
                    cw.writerow([trade.date, opt_id, "", trade.quantity, trade.price, 0])
    else:
        for trades in er.trade_map.values():
            for trade in trades.stock_trades:
                print(trade)
                cw.writerow([trade.date, trade.symbol, "", trade.quantity, trade.price, 0])
    f.close()

    er.capital_gains()

