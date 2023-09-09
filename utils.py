import requests 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def send(token_tg, id_tg, text):
    url = 'https://api.telegram.org/bot'+token_tg+'/sendMessage?chat_id='+id_tg+'&text='+text+''
    resp = requests.get(url)
    r = resp.json()
    return

def sendimage(token_tg, id_tg, img):
    url = 'https://api.telegram.org/bot'+token_tg+'/sendPhoto'
    f = {'photo': open(img, 'rb')}
    d = {'chat_id': id_tg}
    resp = requests.get(url, files = f, data = d)
    r = resp.json()
    return

def timeframe_formatter(timeframe, periods):
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

    return a, days_hours


def charts(sector_name, sectors_df, timeframe, periods):
    plt.figure(figsize=(12, 8))
    if periods is not None:
        a, days_hours = timeframe_formatter(timeframe, periods)
        plt.title(f'{sector_name} relative performance, last {a} {days_hours}, Binance futures')
    else:
        plt.title(f'{sector_name} relative performance, Binance futures')
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