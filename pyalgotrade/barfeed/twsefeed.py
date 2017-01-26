from pyalgotrade.barfeed import csvfeed
from pyalgotrade.barfeed import common
from pyalgotrade.utils import dt
from pyalgotrade import bar

import datetime


######################################################################
## Yahoo Finance CSV parser
# Each bar must be on its own line and fields must be separated by comma (,).
#
# Bars Format:
# Date,Open,High,Low,Close,Volume,Adj Close
#
# The csv Date column must have the following format: YYYY-MM-DD

def parse_date(date):
    # Sample: 2005-12-30
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")

    if len(date) >= 9:  #101/12/12
        year = int(date[0:3]) + 1911
        month = int(date[4:6])
        day = int(date[7:9])
    else:               #98/12/12
        year = int(date[0:2]) + 1911
        month = int(date[3:5])
        day = int(date[6:8])

    ret = datetime.datetime(year, month, day)
    return ret


class RowParser(csvfeed.RowParser):
    def __init__(self, dailyBarTime, frequency, timezone=None, sanitize=False, barClass=bar.BasicBar):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize
        self.__barClass = barClass

    def __parseDate(self, dateString):
        ret = parse_date(dateString)
        # Time on Yahoo! Finance CSV files is empty. If told to set one, do it.
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","

    def parseBar(self, csvRowDict):
        dateTime = self.__parseDate(csvRowDict["Date"].replace(',', ''))
        close = float(csvRowDict["Close"].replace(',', ''))
        open_ = float(csvRowDict["Open"].replace(',', ''))
        high = float(csvRowDict["High"].replace(',', ''))
        low = float(csvRowDict["Low"].replace(',', ''))
        volume = float(csvRowDict["Volume"].replace(',', ''))

        if self.__sanitize:
            open_, high, low, close = common.sanitize_ohlc(open_, high, low, close)

        return self.__barClass(dateTime, open_, high, low, close, volume, None, self.__frequency)


class Feed(csvfeed.BarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from grs.

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    """

    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=None):
        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if frequency not in [bar.Frequency.DAY, bar.Frequency.WEEK]:
            raise Exception("Invalid frequency.")

        super(Feed, self).__init__(frequency, maxLen)

        self.__timezone = timezone
        self.__sanitizeBars = False
        self.__barClass = bar.BasicBar

    def setBarClass(self, barClass):
        self.__barClass = barClass

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromCSV(self, instrument, path, timezone=None):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """

        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None:
            timezone = self.__timezone

        rowParser = RowParser(
            self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars, self.__barClass
        )
        super(Feed, self).addBarsFromCSV(instrument, path, rowParser)
