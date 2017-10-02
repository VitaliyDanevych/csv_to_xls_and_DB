#!/usr/bin/python2.4 -tt
__AUTHOR__='Danevych V.'
__COPYRIGHT__='Danevych V. 2015 Kiev, Ukraine'
#vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

#  #!/usr/bin/env python

import csv
import cx_Oracle
import datetime
                         
db_user, db_passwd, db_host_sid = ('sqXXX','passwdXXX','10.1.34.XXX:1521/orcl')

                          
try:
   con = cx_Oracle.connect(db_user, db_passwd, db_host_sid)
except cx_Oracle.DatabaseError,msg:
   print "Logon  Error:",msg
   sys.exit(1) 

my_cursor = con.cursor()


#SET CORRECT NLS. in another case insert_date will not be work
try:
    my_cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY HH24:MI:SS'")
    my_cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI:SS.FF'")
    print "The NLS_DATE_FORMAT and NLS_TIMESTAMP_FORMAT for DB SESSION are SET"
    my_cursor.execute("COMMIT")
except:
    my_cursor.execute("ROLLBACK")
 
data_from_db = my_cursor.execute("SELECT TO_TIMESTAMP('09.07.2015 16:30:27') FROM DUAL")
print data_from_db

with open('RX_Level_STS.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        for row in reader:
            if row[0] == 'BSC':
                print "First line found, I'll miss it"
                continue
            sql = "INSERT INTO n_rx_level_sts a (a.bsc, a.sitename, a.bcf, a.tre_name, a.fe_num, a.odu_name, a.odu_rx_max,  a.odu_rx_min, a.odu_rx_current, \
            a.odu_rx_average, a.odu_current_tx_power, a.odu_alcq, a.odu_tx_frequency, a.insert_date) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') " \
            % (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
            print sql
            try:
                my_cursor.execute(sql)
                my_cursor.execute("COMMIT")
            except cx_Oracle.DatabaseError,info:
                print "SQL Error: ", '%s' % (sql), info
                pass
            except TypeError as msg:
                #except Exception as msg:
                print "an TypeError occured", msg
                #pass
                my_cursor.execute("ROLLBACK")
                con.close()
           
con.close() 
