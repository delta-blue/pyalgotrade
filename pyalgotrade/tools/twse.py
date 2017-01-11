import os
import datetime
import urllib3
import csv

import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade.barfeed import grsfeed
from pyalgotrade.utils import dt

TWSE_HOST = 'http://www.twse.com.tw/'

def download_csv(instrument, begin):
    result = urllib3.connection_from_url(TWSE_HOST).request('POST',
            '/ch/trading/exchange/STOCK_DAY/STOCK_DAYMAIN.php',
            fields={'download': 'csv',
                    'query_year': begin.year,
                    'query_month': begin.month,
                    'CO_ID': instrument})
    decode_data = result.data.decode('cp950', 'ignore')
    bars = decode_data.encode('utf-8')
    bars = bars.split('\n', 2)[2];
    bars = bars.replace(',\n', '\n')
    return bars


def download_daily_bars(instrument, year, csvFile):
    """Download daily bars from Yahoo! Finance for a given year.

    :param instrument: Instrument identifier.
    :type instrument: string.
    :param year: The year.
    :type year: int.
    :param csvFile: The path to the CSV file to write.
    :type csvFile: string.
    """

    bars = ''
    for i in range(1, 13):
        bars = bars + download_csv(instrument, datetime.date(year, i, 1))
    f = open(csvFile, "w")
    f.write('Date,Volume,Value,Open,High,Low,Close,Diff,Number\n')
    f.write(bars)
    f.close()

def build_feed(instruments, fromYear, toYear, storage, frequency=bar.Frequency.DAY, timezone=None, skipErrors=False):
    """Build and load a :class:`pyalgotrade.barfeed.yahoofeed.Feed` using CSV files downloaded from Yahoo! Finance.
    CSV files are downloaded if they haven't been downloaded before.

    :param instruments: Instrument identifiers.
    :type instruments: list.
    :param fromYear: The first year.
    :type fromYear: int.
    :param toYear: The last year.
    :type toYear: int.
    :param storage: The path were the files will be loaded from, or downloaded to.
    :type storage: string.
    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param skipErrors: True to keep on loading/downloading files in case of errors.
    :type skipErrors: boolean.
    :rtype: :class:`pyalgotrade.barfeed.yahoofeed.Feed`.
    """

    logger = pyalgotrade.logger.getLogger("twse")
    ret = twsefeed.Feed(frequency, timezone)

    if not os.path.exists(storage):
        logger.info("Creating %s directory" % (storage))
        os.mkdir(storage)

    for year in range(fromYear, toYear+1):
        for instrument in instruments:
            fileName = os.path.join(storage, "%s-%d-twse.csv" % (instrument, year))
            if not os.path.exists(fileName):
                logger.info("Downloading %s %d to %s" % (instrument, year, fileName))
                try:
                    if frequency == bar.Frequency.DAY:
                        download_daily_bars(instrument, year, fileName)
                    else:
                        raise Exception("Invalid frequency")
                except Exception, e:
                    if skipErrors:
                        logger.error(str(e))
                        continue
                    else:
                        raise e
            ret.addBarsFromCSV(instrument, fileName)
    return ret
