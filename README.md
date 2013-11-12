
## Install/Configure FreeTDS/UnixODBC on MacOSX

$ sudo port install unixodbc freetds
$ sudo cp setup/freetds.conf /opt/local/etc/freetds/freetds.conf 
$ tsql -S SportsDB -U %USER%

$ port contents freetds | grep /libtdsodbc.so
  /opt/local/lib/libtdsodbc.so

$ sudo odbcinst -d -i -f freetds-driver
odbcinst: Driver installed. Usage count increased to 1. 
    Target directory is /opt/local/etc

$ sudo cp setup/odbc.ini /opt/local/etc/odbc.ini
$ isql -v SportsDB %USER% %PASS%
```
+---------------------------------------+
| Connected!                            |
|                                       |
| sql-statement                         |
| help [tablename]                      |
| quit                                  |
|                                       |
+---------------------------------------+
SQL> SELECT TOP 10 league_key from SMG_Leagues GROUP BY league_key
+-----------------------------------------------------------------------------------------------------+
| league_key                                                                                          |
+-----------------------------------------------------------------------------------------------------+
| l.mlb.com                                                                                           |
| l.mlsnet.com                                                                                        |
| l.nba.com                                                                                           |
| l.ncaa.org.mbasket                                                                                  |
| l.ncaa.org.mfoot                                                                                    |
| l.ncaa.org.wbasket                                                                                  |
| l.nfl.com                                                                                           |
| l.nhl.com                                                                                           |
| l.wnba.com                                                                                          |
+-----------------------------------------------------------------------------------------------------+
SQLRowCount returns 9
9 rows fetched
SQL> 
```

## Install/Test python modules on MacOSX

$ sudo cp /opt/local/etc/odbc.ini /etc/odbc.ini
$ sudo cp /opt/local/etc/odbcinst.ini /etc/odbcinst.ini
$ pip install pyodbc lxml requests
$ python
```
ActivePython 2.7.5.6 (ActiveState Software Inc.) based on
Python 2.7.5 (default, Sep 16 2013, 23:07:15) 
[GCC 4.2.1 (Apple Inc. build 5664)] on darwin
Type "help", "copyright", "credits" or "license" for more information.

>>> import pyodbc
>>> conn = pyodbc.connect('DSN=SportsDB;UID=%USER%;PWD=%PASS%$')
>>> cur = conn.cursor()
>>> cur.execute('SELECT TOP 10 * FROM SMG_Leagues')
<pyodbc.Cursor object at 0x107293150>
>>> cur.fetchone()
(2008, 'l.mlb.com', 'c.american', 'd.alcentral', 'AL', 'American', 1, 'AL Central', 'AL Central', 2)
>>> cur.fetchall()
[(2008, 'l.mlb.com', 'c.american', 'd.alwest', 'AL', 'American', 1, 'AL West', 'AL West', 3), (2008, 'l.mlb.com', 'c.national', 'd.nlcentral', 'NL', 'National', 2, 'NL Central', 'NL Central', 5), (2008, 'l.mlb.com', 'c.national', 'd.nleast', 'NL', 'National', 2, 'NL East', 'NL East', 4), (2008, 'l.mlb.com', 'c.national', 'd.nlwest', 'NL', 'National', 2, 'NL West', 'NL West', 6), (2008, 'l.mlsnet.com', 'c.mls', 'd.eastern', 'MLS', 'MLS', None, 'Eastern', 'Eastern', None), (2008, 'l.mlsnet.com', 'c.mls', 'd.western', 'MLS', 'MLS', None, 'Western', 'Western', None), (2008, 'l.nba.com', 'c.eastern', 'd.atlantic', 'Eastern', 'Eastern', 1, 'Atlantic', 'Atlantic', 1), (2008, 'l.nba.com', 'c.eastern', 'd.central', 'Eastern', 'Eastern', 1, 'Central', 'Central', 2)]

>>> import requests
>>> requests.get('http://www.google.com')
<Response [200]>

>>> from lxml import etree
>>> root = etree.fromstring('<root><node name="value" /></root>')
>>> root.findall('node')[0].attrib
{'name': 'value'}
>>> root.xpath('node/@name')
['value']

>>> ^D
```
