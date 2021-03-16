import os,sys
BASEDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASEDIR)

LOGSDIR = os.path.join(BASEDIR,'logs')
CONFDIR = os.path.join(BASEDIR,'conf')
RECORDDIR = os.path.join(BASEDIR,'recorddata')

Config_file = os.path.join(CONFDIR,'config.ini')
LogConf_file = os.path.join(CONFDIR,'logger.conf')



#CurrentENV#
ENV = {'Region':'US','Env':'prod'}
#ProgramParameters#
PARAMETERS = {'Mssql_Query_Thread':5,'ES_Query_Thread':20,'ES_Query_Interval':24,'Policy_Count_Threshold':100}
#mapping#
SCAN_SOURCE_MAPPING = {'ex':'Exchange Online','sp':'SharePoint Online','od':'OneDrive'}



#mail parameters#
MAILSET = {'Mail_Type':'plain'}#html/plain#
#mail receivers#
RECEIVERS = ['hamm_zhou@trendmicro.com.cn']
