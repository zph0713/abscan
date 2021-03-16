import json
from conf import settings

class SQLBuild(object):
    def __init__(self,ScanSource,Count_Threshold=settings.PARAMETERS['Policy_Count_Threshold']):
        self.Count_Threshold = Count_Threshold
        self.ScanSource = ScanSource

    def countMainSQL(self):

        Count_Main_SQL = "select count(*) from tm_clp_account inner join tm_delegator_account on tm_clp_account.COMPANY_GUID = tm_delegator_account.company_guid " \
                         "where tm_delegator_account.SERVICE_TYPE = 1 and tm_delegator_account.VALID = 1 " \
                         "and (%s) > %d" % (self.optionSource_queryPolicy(),self.Count_Threshold)

        return Count_Main_SQL

    def queryMainSQL(self,top_n,top_m):

        Query_Main_SQL = "select top %d tm_clp_account.COMPANY_GUID from tm_clp_account inner join tm_delegator_account on tm_clp_account.COMPANY_GUID = tm_delegator_account.company_guid " \
                         "where tm_delegator_account.SERVICE_TYPE = 1 and tm_delegator_account.VALID = 1 " \
                         "and tm_clp_account.CREATED_AT not in " \
                         "(select top %d tm_clp_account.CREATED_AT from tm_clp_account inner join tm_delegator_account on tm_clp_account.COMPANY_GUID = tm_delegator_account.company_guid " \
                         "where tm_delegator_account.SERVICE_TYPE = 1 and tm_delegator_account.VALID = 1 order by tm_clp_account.CREATED_AT ) " \
                         "and (%s) >%d " \
                         "order by tm_clp_account.CREATED_AT" % (top_n,top_m,self.optionSource_queryPolicy(),self.Count_Threshold)

        return Query_Main_SQL

    def optionSource_queryPolicy(self):
        ExchangePolicyEnableCount = "select count(*) from tm_user_policy " \
                                    "where (tm_user_policy.policy_id_atp IS NOT null or tm_user_policy.policy_id_dlp IS NOT null) " \
                                    "and tm_user_policy.company_id =tm_clp_account.COMPANY_GUID"

        SharePointPolicyEnableCount = "select count(*) from tm_target_policy_sp " \
                                      "where (tm_target_policy_sp.policy_id_atp IS NOT null or tm_target_policy_sp.policy_id_dlp IS NOT null) " \
                                      "and tm_target_policy_sp.company_id =tm_clp_account.COMPANY_GUID"

        OneDrivePolicyEnableCount = "select count(*) from tm_target_policy_od " \
                                    "where (tm_target_policy_od.policy_id_atp IS NOT null or tm_target_policy_od.policy_id_dlp IS NOT null) " \
                                    "and tm_target_policy_od.company_id =tm_clp_account.COMPANY_GUID"

        if self.ScanSource == json.dumps(settings.SCAN_SOURCE_MAPPING['ex']):
            return ExchangePolicyEnableCount
        elif self.ScanSource == json.dumps(settings.SCAN_SOURCE_MAPPING['sp']):
            return SharePointPolicyEnableCount
        elif self.ScanSource == json.dumps(settings.SCAN_SOURCE_MAPPING['od']):
            return OneDrivePolicyEnableCount

    def getAbscanUserInfo(self,GUID):
        AbscanUserInfoSQL = "SELECT top 1 tm_clp_account.EMAIL,tm_clp_account.USER_DISPLAYNAME,tm_clp_account.COMPANY_GUID,clp_license.expiration_date,clp_license.activation_code,clp_license.seats " \
                            "FROM tm_clp_account INNER JOIN clp_license ON tm_clp_account.CLP_COMPANY_ID = clp_license.companyID " \
                            "WHERE tm_clp_account.COMPANY_GUID = '%s'" % GUID
        MailboxCount = "select count(*) from tm_user where tm_user.COMPANY_GUID = '%s' and tm_user.USER_PRIMARYEMAIL <> ''" % GUID
        Policysql = self.optionSource_queryPolicy()
        PolicyEnableCount = Policysql.replace("tm_clp_account.COMPANY_GUID","'"+GUID+"'")
        sqllist = [AbscanUserInfoSQL,MailboxCount,PolicyEnableCount]      
        return sqllist

class ESBuild(object):
    def __init__(self,ScanSource,TimeInterval=settings.PARAMETERS['ES_Query_Interval']):
        self.ScanSource = ScanSource
        self.TimeInterval = TimeInterval

    def queryES(self,GUID):
        query_body = '{"size":0,"query":{"bool":{"filter":[' \
                     '{"terms":{"tm_company_guid":["%s"]}},' \
                     '{"exists":{"field":"tm_count"}},' \
                     '{"terms":{"tm_scan_source":[%s]}},' \
                     '{"terms":{"tm_module":["scan-scanner","scan-dda","count-scanner"]}},' \
                     '{"range":{"tm_timestamp":{"gte":"now-%dh"}}}]}},' \
                     '"aggs":{"scan_sources":{"terms":{"field":"tm_scan_source"},' \
                     '"aggs":{"sum_scan_count":{"sum":{"field":"tm_count"}}}}}}' % (GUID,self.ScanSource,self.TimeInterval)

        return query_body
