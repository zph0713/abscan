from core import abscan,mail
from conf import settings
import argparse




def recordCheck(scansource,interval,datasource):
    abscan_data = []
    for ss in scansource:
        instance = abscan.Ascan(ss,interval)
        formatdata = instance.queryAbscanDataInfo(ss,datasource)
        for i in formatdata:
            abscan_data.append(i)
    return abscan_data

def allCheck(scansource,interval,datasource,realresult):
    abscan_data = []
    for i in scansource:
        ascan_instance = abscan.Ascan(i,interval)
        ascan_instance.treadQueryMsSQLData()
        ascan_instance.threadQueryESData()
        formatdata = ascan_instance.queryAbscanDataInfo(i,datasource)
        if realresult is False:
            recorddata = ascan_instance.filterLastAbscanfromFile(i)
            for n in formatdata:
                if n['COMPANY_GUID'] not in recorddata:
                    abscan_data.append(n)
        else:
            for m in formatdata:
                abscan_data.append(m)
    return abscan_data

def sendMail(abscan_data):
    if abscan_data != []:
        mail_instance = mail.SendMail()
        message = mail_instance.generateMail(abscan_data)
        mail_instance.sendMail(message)
    else:
        print('scan normal')

def checkAll():
    parser = argparse.ArgumentParser(add_help=True,description='abnormalscan')
    exclusive_group = parser.add_mutually_exclusive_group(required=True)
    exclusive_group.add_argument('-s','--scansource',nargs='+')
    parser.add_argument('-i','--interval',help="interval",type=int,default=24)
    parser.add_argument('-R','--recorddata',help="recorddata",action="store_true")
    parser.add_argument('-r','--realresult',help="realresult",action="store_true")
    obj = parser.parse_args()
    if obj.scansource != None:
        if obj.recorddata is True:
            data = recordCheck(obj.scansource,obj.interval,obj.recorddata)
            sendMail(data)
        else:
            data = allCheck(obj.scansource,obj.interval,obj.recorddata,obj.realresult)
            sendMail(data)

def main():
    checkAll()

if __name__ == '__main__':
    main()
