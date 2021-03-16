import splunklib.client as client
import json


HOST = ""
PORT = 8089
USERNAME = ""
PASSWORD = ""
VERIFY = False


class SplunkAPI(object):
    def __init__(self):
        self._splunk_conn()

    def _splunk_conn(self):
        service = client.connect(
            host=HOST,
            port=PORT,
            username=USERNAME,
            password=PASSWORD,
            verify=VERIFY)
        return service

    def splun_query(self,search_statements):
        jobs = self._splunk_conn().jobs
        job_instance = jobs.create(search_statements, **{"exec_mode": "blocking"})
        result_stream = job_instance.results(output_mode="json")
        json_result = json.loads(result_stream.read())
        log_list = json_result["results"]
        for i in log_list:
            print i


if __name__ == "__main__":
    SAPI = SplunkAPI()
    SAPI.splun_query("search *|head 5")
