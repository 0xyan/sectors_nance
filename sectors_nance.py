import pandas as pd
import asyncio
import matplotlib.pyplot as plt
import schedule
import time
import os
from dotenv import load_dotenv
import matplotlib.dates as mdates
from binance.client import Client
from binance import AsyncClient
from utils import send, sendimage, charts

load_dotenv()

#def main_function(timeframe, startTime=None, periods=None):
    
async def binance_init():
    binance_api_key = os.getenv("BINANCE_API_KEY")
    binance_secret = os.getenv("BINANCE_SECRET")
    client = await AsyncClient.create(binance_api_key, binance_secret)

    return client

def tg_init():
    token_tg = os.getenv("TELEGRAM_TOKEN")
    id_tg = os.getenv("TELEGRAM_ID")

    return token_tg, id_tg

def init_sectors():
    #sectors
    high_FDV = ['APTUSDT', 'FILUSDT',  'SUIUSDT', 'WLDUSDT', 'SEIUSDT']
    chinese = ['NEOUSDT', 'TRXUSDT', 'CFXUSDT', 'ACHUSDT', 'PHBUSDT', 'QTUMUSDT', 'HIGHUSDT', 'ONTUSDT']
    perps = ['GMXUSDT', 'SNXUSDT', 'DYDXUSDT', 'GNSUSDT', 'PERPUSDT']
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

    #Creating a {name:list} of assets dict
    sectors = {name: sector for name, sector in zip(names_list, sector_list)}
    
    return sectors
    

#getting klines function
async def get_klines(client, symbol, interval, startTime=None, limit=None):
    try:
        if startTime is not None and limit is None:
            klines_data = await client.get_klines(symbol=symbol, interval=interval, startTime=startTime)
        elif startTime is None and limit is not None:
            klines_data = await client.get_klines(symbol=symbol, interval=interval, limit=limit)
        elif startTime is not None and limit is not None:
            klines_data = await client.get_klines(symbol=symbol, interval=interval, startTime=startTime, limit=limit)
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
        print(f'error processing {symbol}:{e}')
    return df

    
#creating a dictionary with all dataframes
async def create_sector_dfs(client, sectors, interval, startTime=None, limit=None):
    sector_dict_final = {}

    async def fetch_asset_df(asset):
        try:
            return await get_klines(client, asset, interval, startTime, limit)
        except Exception as e:
            print(f'Error processing {asset}: {e}')
            return None


    # iterate over each sector
    for sector_name, assets in sectors.items():
        # fetch all assets dataframes concurrently within a sector
        tasks = [fetch_asset_df(asset) for asset in assets]
        asset_dfs = await asyncio.gather(*tasks)
        
        # initialize an empty dataframe for the sector
        sector_df = pd.DataFrame()

        # iterate over each asset in the sector
        for asset_df in asset_dfs:
            if asset_df is not None:
                if sector_df.empty:
                    sector_df = asset_df
                else:
                    sector_df = pd.merge(sector_df, asset_df, left_index=True, right_index=True, how='outer')

        # add the sector dataframe to the dictionary
        sector_dict_final[sector_name] = sector_df

    return sector_dict_final

def final_dict_manipulations(final_dict):
    #calculating an equal-weighted return
    for k,v in final_dict.items():
        for i, row in v.iterrows():
            v.loc[i, f'{k}'] = row.mean()

    #creating the dataframe with sectors cum ret
    df_sectors_returns = pd.DataFrame(index = final_dict['Privacy'].index)
    for k,v in final_dict.items():
        df_sectors_returns[f'{k}'] = v.iloc[: , -1]
    
    return(df_sectors_returns)

#get order for charts to plot in descending order
def order(df):
    ordered_series = df[-1::].max()
    ordered_series = ordered_series.sort_values(ascending=False)
    df = df[ordered_series.index.to_list()]

    return(df)

def best_worst_list_func(df_sectors_returns):
    best_sector = df_sectors_returns.columns[0]
    best_sector_2 = df_sectors_returns.columns[1]
    worst_sector = df_sectors_returns.columns[-1]
    worst_sector_2 = df_sectors_returns.columns[-2]
    best_worst_list = [best_sector, best_sector_2, worst_sector, worst_sector_2]

    return best_worst_list

def send_individual_sectors(best_worst_list, final_dict, timeframe, periods, token_tg, id_tg):
    for i in best_worst_list:
        sector_df = order(final_dict[i])
        charts(i, sector_df, timeframe, periods)
        sendimage(token_tg, id_tg, 'mychart_sectors.png')

async def main(timeframe, startTime=None, periods=None):
    client = await binance_init()
    token_tg, id_tg = tg_init()
    sectors = init_sectors()

    try:
        final_dict = await create_sector_dfs(client, sectors=sectors, startTime=startTime, interval=timeframe, limit=periods)
    except Exception as e:
        print(f'error in function final_dict: {e}')

    df_sectors_returns = final_dict_manipulations(final_dict)
    df_sectors_returns = order(df_sectors_returns)
    best_worst_list = best_worst_list_func(df_sectors_returns)

    charts('Equally-weighted sectors', df_sectors_returns, timeframe, periods)
    send(token_tg, id_tg, '%23sectors')
    sendimage(token_tg,id_tg,'mychart_sectors.png')
    send_individual_sectors(best_worst_list, final_dict, timeframe, periods, token_tg, id_tg)

'''
if __name__ == "__main__":

    asyncio.run(main(timeframe='1h', periods=168))

'''
def job():
    asyncio.run(main(timeframe='1h', periods=168))

def setup_schedule():
    schedule.every().day.at("15:28").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    setup_schedule()