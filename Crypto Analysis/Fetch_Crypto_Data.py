from binance.client import Client
import time
import datetime
import sqlite3
import pandas as pd
import pytz
import plotly.graph_objects as go




def run():
    db = r'C:\sqllite\Crypto.db'
    conn = establish_connection(db)
    client = establish_keys(conn)
    SYMBOL = 'ETHUSDT'
    list_of_symbols = ['ETHUSDT']

    # run_for_graph = input("Enter your value: ") 
 
    
    try:
        status = client.get_system_status()

        if(status.get('msg') == 'normal'):
            try:
                conn = establish_connection(db)

                iteration = 0

                if(iteration == 0):
                    fetch_historical_data(conn,client,SYMBOL)

                insert_current_price()
                iteration+=1

                # build_candlestick(df)
            except sqlite3.Error as error:
                print("Failed to connect to the DB", error)
        else:
            print('shit')
    except():
        pass


def insert_current_price(SYMBOL):
    avg_price_dict = client.get_avg_price(symbol=SYMBOL)
    avg_price = avg_price_dict.get('price')
    time_now = datetime.datetime.now().strftime('%m-%d-%Y %H:%M:%S')

    insert_sql = f'''
    insert into HIST_ETHUSDT_DATA (SYMBOL, OPEN_TIME, OPEN_PRICE, HIGH, LOW, CLOSE, VOLUME, CLOSE_TIME
    ,QUOTE_ASSET_VOLUME, NUM_OF_TRADES, TAKER_BUY_BAV, TAKER_BUY_QAV) VALUES 
    ('{SYMBOL}','{convert_ms_to_timestamp(open_time)}','{open_price}','{high}','{low}','{close}','{volume}','{convert_ms_to_timestamp(close_time)}','
    {quote_asset_volume}',{num_of_trades},'{taker_buy_bav}','{taker_buy_qav}')     
    '''       

    open_time = datetime.datetime()
    open_price = kline[1]
    high = kline[2]
    low = kline[3]
    close = kline[4]
    volume = kline[5]
    close_time = kline[6]
    quote_asset_volume = kline[7]
    num_of_trades = kline[8]
    taker_buy_bav = kline[9]
    taker_buy_qav = kline[10]

def fetch_historical_data(conn,client,SYMBOL):

    max_local_dt = get_max_dt(conn)
    
    max_utc_dt = local_to_utc(max_local_dt) + datetime.timedelta(seconds=60)
    
    max_utc_str = max_utc_dt.strftime('%d %b, %Y %H:%M:%S')
    
    klines = client.get_historical_klines(SYMBOL, Client.KLINE_INTERVAL_5MINUTE, max_utc_str)
    
    write_klines(conn,klines,SYMBOL)

    START_DT = None
    END_DT = None

    df = build_analaysis_df(conn, SYMBOL, START_DT, END_DT)

    # return df
  
def build_analaysis_df(conn,symbol,start_dt,end_dt):
    if(start_dt is None or end_dt is None):
        cursor = conn.cursor()
        sql = f'SELECT * FROM HIST_ETHUSDT_DATA WHERE SYMBOL = \'{symbol}\''
        df = pd.read_sql(sql, conn)
        dates = df['OPEN_TIME'].tolist()
        dates_list = [datetime.datetime.strptime(date, '%m/%d/%y %H:%M:%S') for date in dates]
        df['OPEN_DATE'] = dates_list
        
    else:
        print("Get delta DF")
    
    cursor.close()
    return df

def establish_connection(db):
    sqliteConnection = sqlite3.connect(db)
    return sqliteConnection

def establish_keys(conn):
    cursor = conn.cursor()
    sql_str = 'select * from not_keys'
    cursor.execute(sql_str)
    rows = cursor.fetchall()
    cursor.close()

    for tup1,tup2 in rows:
        api_key = tup1
        api_secret = tup2

    client = Client(api_key, api_secret)
    conn.close()
    return client

def write_klines(conn,klines,SYMBOL):
    cursor = conn.cursor()
    for kline in klines:
        open_time = kline[0]
        open_price = kline[1]
        high = kline[2]
        low = kline[3]
        close = kline[4]
        volume = kline[5]
        close_time = kline[6]
        quote_asset_volume = kline[7]
        num_of_trades = kline[8]
        taker_buy_bav = kline[9]
        taker_buy_qav = kline[10]

        insert_sql = f'''
        
        insert into HIST_ETHUSDT_DATA (SYMBOL, OPEN_TIME, OPEN_PRICE, HIGH, LOW, CLOSE, VOLUME, CLOSE_TIME
        ,QUOTE_ASSET_VOLUME, NUM_OF_TRADES, TAKER_BUY_BAV, TAKER_BUY_QAV) VALUES 
        ('{SYMBOL}','{convert_ms_to_timestamp(open_time)}','{open_price}','{high}','{low}','{close}','{volume}','{convert_ms_to_timestamp(close_time)}','
        {quote_asset_volume}',{num_of_trades},'{taker_buy_bav}','{taker_buy_qav}')     
        '''       

        try:
            count = cursor.execute(insert_sql)
            print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)
        except sqlite3.Error as error:
            open_time = convert_ms_to_timestamp(open_time)
            print(f"Failed to insert the following data: {SYMBOL}, {open_time}", error)
    conn.commit()
    cursor.close()
    
def get_max_dt(conn):
    cursor = conn.cursor()

    ''' KLine list indexes
        index 0   // Open time
        index 1   // Open
        index 2   // High
        index 3   // Low
        index 4   // Close
        index 5   // Volume
        index 6   // Close time
        index 7   // Quote asset volume
        index 8   // Number of trades
        index 9   // Taker buy base asset volume
        index 10  // Taker buy quote asset volume
        index 11  // Ignore.
        '''
    sql = 'SELECT * FROM HIST_ETHUSDT_DATA'

    df = pd.read_sql(sql, conn)

    dates = df['OPEN_TIME'].tolist()
    
    dates_list = [datetime.datetime.strptime(date, '%m/%d/%y %H:%M:%S') for date in dates]
    
    max_local_dt = max(dates_list) 

    cursor.close()

    return max_local_dt    

def build_candlestick(df):
    fig = go.Figure(data=[go.Candlestick(
        x=df['OPEN_DATE'],
        open=df['OPEN_PRICE'],
        high=df['HIGH'],
        low=df['LOW'],
        close=df['CLOSE'])])

    fig.update_layout(xaxis_rangeslider_visible=False)
    fig.show()

def local_to_utc(local_dt):
    local = pytz.timezone ("America/New_York")
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt

def convert_ms_to_timestamp(ms):
    timestamp = datetime.datetime.fromtimestamp(ms/1000.0).strftime("%m/%d/%y %H:%M:%S")
    return timestamp

def convert_time_binance(gt):
    aa = str(gt)
    bb = aa.replace("{'serverTime': ","")
    aa = bb.replace("}","")
    gg=int(aa)
    ff=gg-10799260
    uu=ff/1000
    yy=int(uu)
    tt=time.localtime(yy)
    return tt

def market_depth(sym, num_entries=20):
    #Get market depth
    #Retrieve and format market depth (order book) including time-stamp
    i=0     #Used as a counter for number of entries
    print("Order Book: ", convert_time_binance(client.get_server_time()))
    depth = client.get_order_book(symbol=sym)
    print(depth)
    print(depth['asks'][0])
    ask_tot=0.0
    ask_price =[]
    ask_quantity = []
    bid_price = []
    bid_quantity = []
    bid_tot = 0.0
    place_order_ask_price = 0
    place_order_bid_price = 0
    max_order_ask = 0
    max_order_bid = 0
    print("\n", sym, "\nDepth     ASKS:\n")
    print("Price     Amount")
    for ask in depth['asks']:
        if i<num_entries:
            if float(ask[1])>float(max_order_ask):
                #Determine Price to place ask order based on highest volume
                max_order_ask=ask[1]
                place_order_ask_price=round(float(ask[0]),5)-0.0001
            #ask_list.append([ask[0], ask[1]])
            ask_price.append(float(ask[0]))
            ask_tot+=float(ask[1])
            ask_quantity.append(ask_tot)
            #print(ask)
            i+=1
    j=0     #Secondary Counter for Bids
    print("\n", sym, "\nDepth     BIDS:\n")
    print("Price     Amount")
    for bid in depth['bids']:
        if j<num_entries:
            if float(bid[1])>float(max_order_bid):
                #Determine Price to place ask order based on highest volume
                max_order_bid=bid[1]
                place_order_bid_price=round(float(bid[0]),5)+0.0001
            bid_price.append(float(bid[0]))
            bid_tot += float(bid[1])
            bid_quantity.append(bid_tot)
            #print(bid)
            j+=1
    return ask_price, ask_quantity, bid_price, bid_quantity, place_order_ask_price, place_order_bid_price
    #Plot Data

def scalping_orders(coin, wait=1, tot_time=1):
    #Function for placing 'scalp orders'
    #Calls on Visualizing Scalping Orders Function
    ap, aq, bp, bq, place_ask_order, place_bid_order, spread, proj_spread, max_bid, min_ask = visualize_market_depth(wait, tot_time, coin)
    print("Coin: {}\nPrice to Place Ask Order: {}\nPrice to place Bid Order: {}".format(coin, place_ask_order, place_bid_order))
    print("Spread: {} % Projected Spread {} %".format(spread, proj_spread))
    print("Max Bid: {} Min Ask: {}".format(max_bid, min_ask))
    #Place Orders based on calculated bid-ask orders if projected > 0.05% (transaction fee)
    #Documentation: http://python-binance.readthedocs.io/en/latest/account.html#orders
    """
    if proj_spread > 0.05:
        quant1=100          #Determine Code Required to calculate 'minimum' quantity
        #Place Bid Order:
        bid_order1 = client.order_limit_buy(
            symbol=coin,
            quantity=quant1,
            price=place_bid_order)
        #Place Ask Order
        ask_order1 = client.order_limit_sell(
            symbol=coin,
            quantity=quant1,
            price=place_ask_order)
    #Place second order if current spread > 0.05% (transaction fee)
    """

def visualize_market_depth(wait_time_sec='1', tot_time='1', sym='ICXBNB', precision=5):
    cycles = int(tot_time)/int(wait_time_sec)
    start_time = time.asctime()
    fig, ax = plt.subplots()
    for i in range(1,int(cycles)+1):
        ask_pri, ask_quan, bid_pri, bid_quan, ask_order, bid_order = market_depth(sym)

        #print(ask_price)
        plt.plot(ask_pri, ask_quan, color = 'red', label='asks-cycle: {}'.format(i))
        plt.plot(bid_pri, bid_quan, color = 'blue', label = 'bids-cycle: {}'.format(i))

        #ax.plot(depth['bids'][0], depth['bids'][1])
        max_bid = max(bid_pri)
        min_ask = min(ask_pri)
        max_quant = max(ask_quan[-1], bid_quan[-1])
        spread = round(((min_ask-max_bid)/min_ask)*100,5)   #Spread based on market
        proj_order_spread = round(((ask_order-bid_order)/ask_order)*100, precision)
        price=round(((max_bid+min_ask)/2), precision)
        plt.plot([price, price],[0, max_quant], color = 'green', label = 'Price - Cycle: {}'.format(i)) #Vertical Line for Price
        plt.plot([ask_order, ask_order],[0, max_quant], color = 'black', label = 'Ask - Cycle: {}'.format(i))
        plt.plot([bid_order, bid_order],[0, max_quant], color = 'black', label = 'Buy - Cycle: {}'.format(i))
        #plt.plot([min_ask, min_ask],[0, max_quant], color = 'grey', label = 'Min Ask - Cycle: {}'.format(i))
        #plt.plot([max_bid, max_bid],[0, max_quant], color = 'grey', label = 'Max Buy - Cycle: {}'.format(i))
        ax.annotate("Max Bid: {} \nMin Ask: {}\nSpread: {} %\nCycle: {}\nPrice: {}"
                    "\nPlace Bid: {} \nPlace Ask: {}\n Projected Spread: {} %".format(max_bid, min_ask, spread, i, price, bid_order, ask_order, proj_order_spread),
                    xy=(max_bid, ask_quan[-1]), xytext=(max_bid, ask_quan[0]))
        if i==(cycles+1):
            break
        else:
            time.sleep(int(wait_time_sec))
    #end_time = time.asctime()
    ax.set(xlabel='Price', ylabel='Quantity',
       title='Binance Order Book: {} \n {}\n Cycle Time: {} seconds - Num Cycles: {}'.format(sym, start_time, wait_time_sec, cycles))
    plt.legend()
    plt.show()
    return ask_pri, ask_quan, bid_pri, bid_quan, ask_order, bid_order, spread, proj_order_spread, max_bid, min_ask

def coin_prices(watch_list):
    #Will print to screen, prices of coins on 'watch list'
    #returns all prices
    prices = client.get_all_tickers()
    print("\nSelected (watch list) Ticker Prices: ")
    for price in prices:
        if price['symbol'] in watch_list:
            print(price)
    return prices

def coin_tickers(watch_list):
    # Prints to screen tickers for 'watch list' coins
    # Returns list of all price tickers
    tickers = client.get_orderbook_tickers()
    print("\nWatch List Order Tickers: \n")
    for tick in tickers:
        if tick['symbol'] in watch_list:
            print(tick)
    return tickers

def portfolio_management(deposit = '10000', withdraw=0, portfolio_amt = '0', portfolio_type='USDT', test_acct='True'):
    """The Portfolio Management Function will be used to track profit/loss of Portfolio in Any Particular Currency (Default: USDT)"""
    #Maintain Portfolio Statistics (Total Profit/Loss) in a file
    pass

def Bollinger_Bands():
    #This Function will calculate Bollinger Bands for Given Time Period
    #EDIT: Will use Crypto-Signal for this functionality
    #https://github.com/CryptoSignal/crypto-signal
    pass

def buy_sell_bot():
    pass

def position_sizing():
    pass

def trailing_stop_loss():
    pass

if __name__ == "__main__":
    run()

