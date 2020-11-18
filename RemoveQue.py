import pyodbc
import zeep
import numpy as np


# Logon to Navitaire api
def logon():
    try:
        status = False
        req = zeep.Client('URL')

        data_se = {
            "ACCESS_INFO": 'info',
        }

        header_se = {
            "HEADER": 'info',
        }

        signature = req.service.Logon(data_se, _soapheaders=header_se)
        return signature
    except Exception as er:
        print('from api sing')
        print(er)
# Logout method from Navitaire api


def logout(sing):
    try:
        req = zeep.Client('URL')

        data_se = {
            "ACCESS_INFO": 'info',
        }
        header_se = {
            "HEADER": 'info'
        }
        logout = req.service.Logout(data_se, _soapheaders=header_se)
        return logout
    except Exception as er:
        print('from api sing')
        print(er)

# Get Booking IDs From Navitaire  DB MSSQL
# Selecting all OUTBAL Queue


def getBookingIDs():
    try:
        conn = pyodbc.connect("DATABASE INFO")
        cursor = conn.cursor()
        cursor.execute("SELECT QUERY")
        results = cursor.fetchall()
        results = np.asarray(results)
        # print(results)
        cursor.close()
        conn.close()
        return results
    except Exception as er:
        print(er)

# Delete Method for OUTBAL Queue has 0 outstanding Balance Navitaire API


def deletBookingFromQueue(id, date):
    sing = logon()
    try:
        req = zeep.Client('URL')

        data = {
            "QueueCode" 'info',
        }

        header = {
            "HEADER": 'info'
        }

        deleteQueue = req.service.DeleteBookingFromQueue(
            data, _soapheaders=header)
        logout(sing)
        print(deleteQueue)
    except Exception as er:
        print('from api Dele')
        print(er)

# initionalization Function to get ids, verify and delete


def init():
    # get IDs DB
    ids = getBookingIDs()
    # logon
    sing = logon()
    for id in ids:
        try:
            print(id)
            req = zeep.Client('URL')

            data = {
                "DATA Req": 'info'
            }
            header = {
                "HEADER":  'info'
            }
          # GEt booking Deteils
            getBooking = req.service.GetBooking(data, _soapheaders=header)
            # check outstanding balance, must be == 0 in order to delete qeueu
            if (getBooking.BookingSum.BalanceDue == 0 and getBooking.BookingSum.AuthorizedBalanceDue == 0):
                print(getBooking.BookingID)
                # logout
                logout(sing)
                # Fire delete Qeueu Method
                deletBookingFromQueue(
                    getBooking.BookingID, getBooking.BookingInfo.CreatedDate)
        except Exception as er:
            print('from api get bookng')
            print(er)


init()
