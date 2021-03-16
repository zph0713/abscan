import splunklib.client as client
import splunklib.results as results
import json


HOST = ""
PORT = 8089
USERNAME = ""
PASSWORD = ""
VERIFY = False

# Create a Service instance and log in 
service = client.connect(
    host=HOST,
    port=PORT,
    username=USERNAME,
    password=PASSWORD,
    verify=VERIFY)




search_statements = "search * | head 5"
query_parameter = {"exec_mode": "blocking"}

jobs = service.jobs
job = jobs.create(search_statements, **query_parameter)


result_stream = job.results(output_mode="json")
json_result = json.loads(result_stream.read())
log_list = json_result["results"]
for i in log_list:
    print i
