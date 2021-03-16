import ConfigParser
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from jinja2 import Environment,PackageLoader

from conf import settings
from logger import Logger

LOG = Logger('mailreport')




class SendMail(object):
    def __init__(self):
        self._getConfig()

    def _getConfig(self):
        config = ConfigParser.RawConfigParser()
        config.read(settings.Config_file)
        ##MailServerInfo##
        self.mail_server = config.get('mail_server','smtp_server')
        self.mail_port = config.get('mail_server','port')
        self.mail_user = config.get('mail_server','smtp_user')
        self.mail_passwd = config.get('mail_server','smtp_passwd')
        self.source_addr = config.get('mail_server','source_address')

    def generateMail(self,abscan_data):
        LOG.info('generate mail report ..')
        mail_type = settings.MAILSET['Mail_Type']
        if abscan_data != []:
            if mail_type == 'plain':
                mails_text = []
                for data in abscan_data:
                    MAIL_TEMPLATE = '''-----------------\nScanSource: %s\nCLP_Account: %s  Register_Email: %s  \nMailBox_Count: %s  Expiration_Data: %s  Policy_Enable_Count: %s  \nCompany_Guid: %s   Activation_Code: %s \n''' % (
                    settings.SCAN_SOURCE_MAPPING[data['SCANSOURCE']],data['USER_DISPLAYNAME'], data['Register_Email'], data['MAILBOX_COUNT'], data['EXPIRATION_DATA'],data['POLICY_ENABLE_COUNT'], 
                    data['COMPANY_GUID'], data['ACTIVATION_CODE'])
                    mails_text.append(MAIL_TEMPLATE)
                mails_text = '\n'.join(mails_text)
                LOG.info(mails_text)
                return mails_text
            elif mail_type == 'html':
                env = Environment(loader=FileSystemLoader(settings.CONFDIR))
                template = env.get_template('mail_template.html')
                envregion = settings.ENV['Env'] + '-' + settings.ENV['Region']
                #html_content = template.render(envregion=envregion,scansource=settings.SCAN_SOURCE_MAPPING[settings.ENV['ScanSource']],user_dict_list=abscan_data)
                html_content = template.render(envregion=envregion,scansource=self.scan_source,user_dict_list=abscan_data)
                return html_content

    def sendMail(self,Message):
        maillog = 'send to %s' %(settings.RECEIVERS)
        LOG.info(maillog)
        Subject = "[TMCAS][%s][%s]Abnormal Scan Accounts" % (settings.ENV['Env'],settings.ENV['Region'])
        if Message != None:
            message = MIMEMultipart()
            Receivers = ';'
            message['From'] = self.source_addr
            message['To'] = Header(Receivers.join(settings.RECEIVERS))
            message['Subject'] = Header(Subject,'utf-8')
            message.attach(MIMEText(Message,settings.MAILSET['Mail_Type'],'utf-8'))
            try:
                smtpObj = smtplib.SMTP()
                #smtpObj.set_debuglevel(1)
                smtpObj.connect(self.mail_server,self.mail_port)
                smtpObj.ehlo()
                smtpObj.starttls()
                smtpObj.login(self.mail_user,self.mail_passwd)
                smtpObj.sendmail(self.source_addr,settings.RECEIVERS,message.as_string())
                LOG.info('mail send successfull')
                smtpObj.quit()
            except smtplib.SMTPException as e:
                LOG.error(e)
        else:
            print('no data')
