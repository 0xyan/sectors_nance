import pandas as pd
import matplotlib.pyplot as plt
import requests
import schedule
import time
import os
import matplotlib.dates as mdates
from binance.client import Client

def main_function(timeframe, startTime=None, periods=None):
    
    binance_api_key = ''
    binance_secret = ''
    
    client = Client(binance_api_key, binance_secret)
    
    #telegram API
    token = ''
    id_tg = ''

    def send(text):
        url = 'https://api.telegram.org/bot'+token+'/sendMessage?chat_id='+id_tg+'&text='+text+''
        resp = requests.get(url)
        r = resp.json()
        return

    def sendimage(img):
        url = 'https://api.telegram.org/bot'+token+'/sendPhoto'
        f = {'photo': open(img, 'rb')}
        d = {'chat_id': id_tg}
        resp = requests.get(url, files = f, data = d)
        r = resp.json()
        return r
    
    #sectors
    high_FDV = ['APTUSDT', 'FILUSDT',  'SUIUSDT', 'WLDUSDT', 'SEIUSDT']
    chinese = ['NEOUSDT', 'TRXUSDT', 'CFXUSDT', 'ACHUSDT', 'PHBUSDT', 'QTUMUSDT', 'HIGHUSDT', 'ONTUSDT']
    perps = ['GMXUSDT', 'SNXUSDT', 'DYDXUSDT', 'GNSUSDT']
    L2s = ['ARBUSDT', 'MATICUSDT', 'OPUSDT']
    privacy = ['XMRUSDT', 'ZECUSDT', 'DASHUSDT']
    storage = ['FILUSDT', 'ARUSDT', 'STORJUSDT']
    btc_forks = ['LTCUSDT', 'BCHUSDT', 'ZECUSDT', 'RVNUSDT', 'DASHUSDT']
    btc_eth = ['BTCUSDT', 'ETHUSDT']
    metaverse_games = ['SANDUSDT', 'MANAUSDT', 'ENJUSDT', 'APEUSDT', 'GALAUSDT', 'AXSUSDT', 'FLOWUSDT', 
                       'IMXUSDT', 'HIGHUSDT', 'ALICEUSDT', 'GMTUSDT', 'MAGICUSDT']
    meme = ['SHIBUSDT', 'DOGEUSDT', 'FLOKIUSDT', 'PEPEUSDT']
    DeFi = ['UNIUSDT', 'AAVEUSDT', 'MKRUSDT', 'SNXUSDT', 'CRVUSDT', 'CVXUSDT', 'LDOUSDT',
                 'DYDXUSDT','1INCHUSDT', 'COMPUSDT', 'BALUSDT', 'YFIUSDT', 'ZRXUSDT', 'GMXUSDT', 'FXSUSDT']
    alt_L1_2020 = ['SOLUSDT', 'NEARUSDT', 'ICPUSDT', 'FTMUSDT', 'ATOMUSDT', 'DOTUSDT', 'AVAXUSDT', 'ALGOUSDT', 'HBARUSDT']
    AI = ['RNDRUSDT', 'FETUSDT', 'OCEANUSDT']
    recent_launchpads = ['CYBERUSDT', 'ARKMUSDT', 'MAVUSDT', 'SEIUSDT', 'EDUUSDT', 'IDUSDT']
    BNB = ['BNBUSDT']
    lsd = ['LDOUSDT', 'RPLUSDT', 'FXSUSDT', 'ANKRUSDT']
    
    #matching names
    sector_list = [high_FDV, chinese, perps, L2s, privacy, storage, 
               btc_forks, btc_eth, metaverse_games, meme, DeFi, alt_L1_2020, AI, recent_launchpads, lsd]
    names_list = ['Low_float_high_FDV', 'Chinese_coins', 'Perps', 'L2s', 'Privacy', 'Storage', 'BTC_forks', 
                  'BTC+ETH', 'Metaverse', 'Meme', 'DeFi_1.0', 'L1s_2020gen', 'AI', 'recent_launchpads', 'LSDs']
    
    #Creating a {name:list of assets dict}
    sectors = {}
    for i in range(len(sector_list)):
        sectors[names_list[i]] = sector_list[i]
        
    
    #getting klines function
    def get_klines(symbol, interval, startTime=None, limit=None):
        try:
            if startTime is not None and limit is None:
                klines_data = client.get_klines(symbol=symbol, interval=interval, startTime=startTime)
            elif startTime is None and limit is not None:
                klines_data = client.get_klines(symbol=symbol, interval=interval, limit=limit)
            elif startTime is not None and limit is not None:
                klines_data = client.get_klines(symbol=symbol, interval=interval, startTime=startTime, limit=limit)
            else:
                raise ValueError('either startTime or limit must be provided')
            
            dfi = pd.DataFrame(klines_data)
            df = pd.DataFrame()
            df['time'] = dfi[0].astype(float)
            df['time'] = pd.to_datetime(df['time'], unit = 'ms')
            df['close'] = dfi[4].astype(float)
            df['return'] = df['close'].pct_change(1)
            df[f'{symbol}'] = (df['return'] + 1).cumprod() - 1
            #set_index
            datetime_series = df['time']
            datetime_index = pd.DatetimeIndex(datetime_series.values)
            df = df.set_index(datetime_index)
            df = df[f'{symbol}']
            df.dropna(inplace = True)
        except Exception as e:
            print(f'error processing{symbol}:{e}')
        return df
    
        
    #creating a dictionary with all dataframes with
    def create_sector_dfs(sectors, interval, startTime=None, limit=None):
        sector_dict_final = {}

        # iterate over each sector
        for sector_name, assets in sectors.items():
            # initialize an empty dataframe for the sector
            sector_df = pd.DataFrame()

            # iterate over each asset in the sector
            for asset in assets:
                
                try:
                    # get the asset dataframe
                    asset_df = get_klines(asset, interval, startTime, limit)

                    # if the sector dataframe is empty, set it to the asset dataframe
                    if sector_df.empty:
                        sector_df = asset_df
                    else:
                        # otherwise, merge the asset dataframe with the sector dataframe
                        sector_df = pd.merge(sector_df, asset_df, left_index=True, right_index=True, how='outer')
                except:
                    pass

            # add the sector dataframe to the dictionary
            sector_dict_final[sector_name] = sector_df

        return sector_dict_final
    
    #calling a function
    final_dict = create_sector_dfs(sectors, interval=timeframe, startTime=startTime, limit=periods)

    #calculating an equal-weighted return
    for k,v in final_dict.items():
        for i, row in v.iterrows():
            v.loc[i, f'{k}'] = row.mean()

    #creating the dataframe with sectors cum ret
    df_sectors_returns = pd.DataFrame(index = final_dict['Privacy'].index)
    for k,v in final_dict.items():
        df_sectors_returns[f'{k}'] = v.iloc[: , -1]

    #changing order of columns by performance
    def order(df):
        ordered_series = df[-1::].max()
        ordered_series = ordered_series.sort_values(ascending=False)
        df = df[ordered_series.index.to_list()]
        return(df)
    
    df_sectors_returns = order(df_sectors_returns)
    
    #best_worst_perform
    best_sector = df_sectors_returns.columns[0]
    best_sector_2 = df_sectors_returns.columns[1]
    best_sector_3 = df_sectors_returns.columns[2]
    best_sector_4 = df_sectors_returns.columns[3]
    best_sector_5 = df_sectors_returns.columns[4]
    best_sector_6 = df_sectors_returns.columns[5]

    worst_sector = df_sectors_returns.columns[-1]
    worst_sector_2 = df_sectors_returns.columns[-2]
    worst_sector_3 = df_sectors_returns.columns[-3]
    worst_sector_4 = df_sectors_returns.columns[-4]
    worst_sector_5 = df_sectors_returns.columns[-5]
    worst_sector_6 = df_sectors_returns.columns[-6]

    best_worst_list = [best_sector, best_sector_2, best_sector_3, best_sector_4, best_sector_5, best_sector_6,
                       worst_sector, worst_sector_2, worst_sector_3, worst_sector_4, worst_sector_5, worst_sector_6]
    
    
    #candlesticks to hour format
    if timeframe == '5m':
        a = int((periods / 12))
        days_hours = 'hours'
    if timeframe == '15m':
        a = int((periods / 4))
        days_hours = 'hours'
    if timeframe == '1h':
        a = int(periods / 24)
        days_hours = 'days'
    if timeframe == '4h':
        a = int((periods / 6))
        days_hours = 'days'
    if timeframe == '1d':
        a = int(periods)
        days_hours = 'days'
        
    
    def charts(sector_name, sectors_df, timeframe):   
        plt.figure(figsize=(12, 8))
        plt.title(f'{sector_name} relative performance, last {a} {days_hours}, Binance futures')
        for sector in sectors_df:
            if sector != 'BTCUSDT':
                plt.plot(sectors_df.index, sectors_df[sector], label = f'{sector} {round(sectors_df[sector][-1]*100, 2)}%')
                plt.annotate(sector, xy=(0.95, sectors_df[sector][-1]), xytext=(8, 0), 
                     xycoords=('axes fraction', 'data'), textcoords='offset points', size = 10, weight = 'bold')
                plt.legend(loc = 'best')
            else:
                plt.plot(sectors_df.index, sectors_df[sector], label = f'{sector} {round(sectors_df[sector][-1]*100, 2)}%', linestyle = 'dashed')
                plt.annotate(sector, xy=(0.95, sectors_df[sector][-1]), xytext=(8, 0), 
                     xycoords=('axes fraction', 'data'), textcoords='offset points', size = 10, weight = 'bold')
                plt.legend(loc = 'best')
        plt.xlabel('Datetime UTC', fontsize = 13)
        plt.ylabel('Return', fontsize = 13)
        plt.grid(axis='y')

        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m %H:%M'))

        plt.savefig('mychart.png')
        os.rename('mychart.png', 'mychart_sectors.png')

        return

    charts('Equally-weighted sectors', df_sectors_returns, timeframe)
    send('%23sectors')
    sendimage('mychart_sectors.png')

    for i in best_worst_list:
        sector_df = order(final_dict[i])
        charts(i, sector_df, timeframe)
        sendimage('mychart_sectors.png')
