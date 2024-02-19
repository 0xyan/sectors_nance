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
    AI = ['RNDRUSDT', 'FETUSDT', 'OCEANUSDT', 'WLDUSDT', 'AIUSDT', 'NFPUSDT']
    old_launchpads = ['CYBERUSDT', 'ARKMUSDT', 'MAVUSDT', 'EDUUSDT', 'IDUSDT']
    recent_launchpads = ['ALTUSDT', 'MANTAUSDT', 'XAIUSDT', 'AIUSDT']
    modular = ['TIAUSDT', 'ALTUSDT', 'DYMUSDT', 'MANTAUSDT']
    solana = ['SOLUSDT', 'JUPUSDT', 'PYTHUSDT', 'JTOUSDT']
    new_listings = ['DYMUSDT', 'RONINUSDT', 'JUPUSDT', 'ALTUSDT', 'ONDOUSDT', 'XAIUSDT', 'AIUSDT', 'NFPUSDT', 'ACEUSDT', 'JTOUSDT']
    bitcoin_eco = ['BTCUSDT', 'ORDIUSDT', '1000SATSUSDT', 'STXUSDT']
    new_non_evm = ['SUIUSDT', 'SEIUSDT', 'APTUSDT']
    perps = ['GMXUSDT', 'SNXUSDT', 'DYDXUSDT', 'PERPUSDT']
    bluechip_L2s = ['ARBUSDT', 'MATICUSDT', 'OPUSDT']
    btc_eth = ['BTCUSDT', 'ETHUSDT']
    gamefi = ['BEAMXUSDT', 'XAIUSDT', 'RONINUSDT', 'ACEUSDT', 'IMXUSDT', 'FLOWUSDT', 'GMTUSDT', 'AXSUSDT', 'APEUSDT',
              'BIGTIMEUSDT', 'SANDUSDT', 'ILVUSDT', 'GALAUSDT']
    meme = ['1000SHIBUSDT', 'DOGEUSDT', '1000FLOKIUSDT', '1000PEPEUSDT', '1000BONKUSDT']
    DeFi = ['UNIUSDT', 'AAVEUSDT', 'MKRUSDT', 'SNXUSDT', 'CRVUSDT', 'CVXUSDT', 'LDOUSDT',
                    'DYDXUSDT','1INCHUSDT', 'COMPUSDT', 'BALUSDT', 'YFIUSDT', 'ZRXUSDT', 'GMXUSDT', 'FXSUSDT']
    alt_L1_2020 = ['SOLUSDT', 'NEARUSDT', 'ICPUSDT', 'FTMUSDT', 'ATOMUSDT', 'DOTUSDT', 'AVAXUSDT', 'ALGOUSDT', 'HBARUSDT']
    #pow = ['LTCUSDT', 'BCHUSDT', 'ZECUSDT', 'RVNUSDT', 'DASHUSDT', 'ETHWUSDT']
    #chinese = ['NEOUSDT', 'TRXUSDT', 'CFXUSDT', 'ACHUSDT', 'PHBUSDT', 'QTUMUSDT', 'HIGHUSDT', 'ONTUSDT']



    #matching names
    sector_list = [AI, old_launchpads, recent_launchpads, modular, solana, new_listings, bitcoin_eco,
                new_non_evm, perps, bluechip_L2s, btc_eth, gamefi, meme, DeFi, alt_L1_2020]
    names_list = ['AI', 'old_launchpads', 'recent_launchpads', 'modular', 'solana_ecosystem', 'new_listings', 'bitcoin_eco',
                    'new_L1_non_evm', 'perps', 'bluechip_L2s', 'btc_eth', 'gamefi', 'meme', 'DeFi', 'alt_L1_2020']

    #Creating a {name:list} of assets dict
    sectors = {name: sector for name, sector in zip(names_list, sector_list)}
    
    return sectors
    

#getting klines function
async def get_klines(client, symbol, interval, start_str=None, limit=None):
    df = pd.DataFrame()
    try:
        if start_str is not None and limit is not None:
            klines_data = await client.futures_continous_klines(pair=symbol, interval=interval, contractType='PERPETUAL', start_str=start_str, limit=limit)
        elif start_str is not None and limit is None:
            klines_data = await client.futures_continous_klines(pair=symbol, interval=interval, contractType='PERPETUAL', start_str=start_str)
        elif start_str is None and limit is not None:
            klines_data = await client.futures_continous_klines(pair=symbol, interval=interval, contractType='PERPETUAL', limit=limit)
        else:
            raise ValueError('start_str or limit has to be provided')
        
        dfi = pd.DataFrame(klines_data)
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
        print(f'error processing {symbol}: {e}')
    
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
    df_sectors_returns = pd.DataFrame(index = final_dict['perps'].index)
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
    best_sector_3 = df_sectors_returns.columns[2]
    worst_sector = df_sectors_returns.columns[-1]
    #worst_sector_2 = df_sectors_returns.columns[-2]
    #worst_sector_3 = df_sectors_returns.columns[-3]
    best_worst_list = [best_sector, best_sector_2, best_sector_3, worst_sector]

    return best_worst_list

def send_individual_sectors(best_worst_list, final_dict, timeframe, periods, token_tg, id_tg):
    for i in best_worst_list:
        sector_df = order(final_dict[i])
        charts(i, sector_df, timeframe, periods)
        sendimage(token_tg, id_tg, 'mychart.png')

async def main(timeframe, startTime=None, periods=None):
    client = await binance_init()
    try:
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
        sendimage(token_tg,id_tg,'mychart.png')
        send_individual_sectors(best_worst_list, final_dict, timeframe, periods, token_tg, id_tg)
    finally:
        await client.close_connection()

def week():
    asyncio.run(main(timeframe='1h', periods=168))

def three_days():
    asyncio.run(main(timeframe='15m', periods=288))

def month():
    asyncio.run(main(timeframe='4h', periods=180))

def setup_schedule():
    schedule.every().wednesday.at("12:00").do(week)
    schedule.every().saturday.at("12:00").do(week)
    schedule.every().day.at("08:00").do(three_days)
    schedule.every().monday.at("12:00").do(month)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    setup_schedule()
