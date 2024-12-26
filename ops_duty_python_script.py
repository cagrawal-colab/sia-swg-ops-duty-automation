import os
import sys
import subprocess
import pandas as pd
from io import StringIO
import time

import glob

def get_current_directory():
    result = subprocess.run(['pwd'], capture_output=True, text=True)
    return result.stdout.strip()


def get_latest_kubeconfig():
    # Directory to search in
    current_directory = get_current_directory()
    directory = os.path.expanduser(current_directory)
    
    # Pattern to match files with the substring
    pattern = os.path.join(directory, "lke-colab-prod-kubeconfig*.yaml")
    
    # Get a list of matching files
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError("No files matching the pattern found.")
    
    # Find the latest file by modification time
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


### Example - python3 ops_duty_script.py 1734892200 1734978600 FARSIGHT-61479
if len(sys.argv)<4:
    print("Usage: python ops_duty_script.py <epoch_start_timestamp> <epoch_end_timestamp> <FARSIGHT-TICKET>")
    sys.exit(1)
# Expand home directory for KUBECONFIG

try:
    latest_kubeconfig = get_latest_kubeconfig()
    kubeconfig_path = os.path.expanduser(latest_kubeconfig)
    os.environ["KUBECONFIG"] = kubeconfig_path
except FileNotFoundError as e:
    print(e)

# Define the namespace, pod name, and query
namespace = "clickhouse"
pod_name = "chi-clickhouse-clickhouse-0-0-0"
query = """
WITH stats AS (
    SELECT 
        L3R.Customer_Id as customer_id,
        COALESCE(NULLIF(CV2.ACCOUNT, ''), CAST(L3R.Customer_Id AS VARCHAR)) AS customer_id_name, 
        SUM(IF(error = true, flows, 0)) AS false_conns, 
        SUM(flows) AS total_conns,
        CONCAT(CAST(SUM(IF(error = true, flows, 0)) AS VARCHAR), '/', CAST(SUM(flows) AS VARCHAR)) AS failure_total_conns,
        CASE 
            WHEN SUM(flows) > 0 THEN (SUM(IF(error = true, flows, 0)) * 100.0 / SUM(flows))
            ELSE 0
        END AS percentage
    FROM 
        colab.l3_agg AS L3R
    GLOBAL LEFT JOIN colab.CustomerTableV2 AS CV2 
        ON L3R.Customer_Id = CV2.ETP_CONFIG_ID
    WHERE 
        15min_client_ts >= toDateTime({}) AND 15min_client_ts <= toDateTime({})
    GROUP BY 
        customer_id,customer_id_name
    ORDER BY 
        total_conns DESC
)
SELECT 
    customer_id,
    customer_id_name,
    CONCAT(
        CAST(customer_id_name AS VARCHAR), ' : ', 
        failure_total_conns, ' : ', 
        CAST(ROUND(percentage, 2) AS VARCHAR), '%'
    ) AS customer_stats 
FROM 
    stats
LIMIT 5
""".format(sys.argv[1], sys.argv[2])

per_customer_stats = """
WITH l3r AS (
    SELECT flows, error, modules, Category, L3R.Drop_reason AS Drop_reason
    FROM colab.l3_agg AS L3R
    GLOBAL LEFT JOIN colab.Drop_Reasons AS DR ON L3R.Drop_reason = DR.Drop_reason
    GLOBAL LEFT JOIN colab.NevadaRegionGeoTable AS NGRT ON L3R.nevada_region = CAST(NGRT.nevada_region AS VARCHAR)
    WHERE 
        15min_client_ts >= toDateTime({}) AND 15min_client_ts <= toDateTime({})
        AND ('{}' = '-1' OR L3R.Customer_Id IN ({}))
)
SELECT 
   metric,
   CONCAT(
       CAST(flows_count AS VARCHAR),
       ':',
       CAST(ROUND(percentage, 2) AS VARCHAR),
       '%'
   ) AS value
FROM (
   SELECT 
       'Customer Configuration Issues' AS metric,
       IFNULL(SUM(CASE WHEN Category ilike '%Customer Configuration Not Complete%' AND error = true THEN flows END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Category ilike '%Customer Configuration Not Complete%' AND error = true THEN flows END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
   UNION ALL
   SELECT 
       'ETP Issues - Known issues' AS metric,
       IFNULL(SUM(CASE WHEN Category ilike '%ETP Issues%' AND error = true THEN flows END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Category ilike '%ETP Issues%' AND error = true THEN flows END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
   UNION ALL
   SELECT 
       'ETP Issues - Unknown issues' AS metric,
       IFNULL(SUM(CASE WHEN Drop_reason = '' AND error = true THEN flows ELSE 0 END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Drop_reason = '' AND error = true THEN flows ELSE 0 END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
   UNION ALL
   SELECT 
       'ETP Potential MITM Issues' AS metric,
       IFNULL(SUM(CASE WHEN Drop_reason = '' AND error = true AND modules ilike '%tlsd%' THEN flows ELSE 0 END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Drop_reason = '' AND error = true AND modules ilike '%tlsd%' THEN flows ELSE 0 END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
   UNION ALL
   SELECT 
       'Client/Application Issues' AS metric,
       IFNULL(SUM(CASE WHEN Category ilike '%Client/Application Issues%' AND error = true THEN flows END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Category ilike '%Client/Application Issues%' AND error = true THEN flows END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
   UNION ALL
   SELECT 
       'Origin Issues' AS metric,
       IFNULL(SUM(CASE WHEN Category ilike '%Origin Issues%' AND error = true THEN flows ELSE 0 END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Category ilike '%Origin Issues%' AND error = true THEN flows ELSE 0 END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
   UNION ALL
   SELECT 
       'Policy Issues' AS metric,
       IFNULL(SUM(CASE WHEN Category ilike '%Valid Policy Drops%' AND error = true THEN flows ELSE 0 END), 0) AS flows_count,
       IFNULL(SUM(CASE WHEN Category ilike '%Valid Policy Drops%' AND error = true THEN flows ELSE 0 END), 0) * 100.0 /
       IFNULL(SUM(flows), 1) AS percentage
   FROM l3r
) subquery
ORDER BY percentage DESC;
"""

username = "svanapar"
password = "eu3MICSoLGdlGrztSGvAuoNP"

test_command = ["kubectl", "get", "pods", "-n", namespace]
# Construct the kubectl exec command
def cli_getter(query=None):
    kubectl_command = [
        "kubectl", "-n", namespace, "exec", '-it', pod_name, "--",
        "clickhouse-client", "-u", username, "--password", password, "--query", query
    ]
    return kubectl_command


# Execute the command
try:
    print(f"Using KUBECONFIG: {os.environ['KUBECONFIG']}")
    command = cli_getter(query)

    result = subprocess.run(command, check=True, text=True, capture_output=True)
    # Convert the raw output into a DataFrame
    data = StringIO(result.stdout)
    column_names = ["customer_id","customer_id_name","customer_stats"]
    df = pd.read_csv(data, sep="\t", header=None,names=column_names)  # Assuming TSV (Tab-Separated Values)

    # Print the DataFrame
    top_customer_drops = "{} \n {} \n {} \n {} \n {} \n\n".format(df.iloc[0]['customer_stats'] , df.iloc[1]['customer_stats'] , df.iloc[2]['customer_stats'],df.iloc[3]['customer_stats'],df.iloc[4]['customer_stats'])
    top_customer_id = [int(df.iloc[0]['customer_id']) , int(df.iloc[1]['customer_id']) , int(df.iloc[2]['customer_id']),int(df.iloc[3]['customer_id']),int(df.iloc[4]['customer_id'])]
    per_customer_stats1 = per_customer_stats.format(sys.argv[1],sys.argv[2],str(top_customer_id[0]),str(top_customer_id[0]))
    per_customer_stats2 = per_customer_stats.format(sys.argv[1],sys.argv[2],str(top_customer_id[1]),str(top_customer_id[1]))
    per_customer_stats3 = per_customer_stats.format(sys.argv[1],sys.argv[2],str(top_customer_id[2]),str(top_customer_id[2]))
    per_customer_stats4 = per_customer_stats.format(sys.argv[1],sys.argv[2],str(top_customer_id[3]),str(top_customer_id[3]))
    per_customer_stats5 = per_customer_stats.format(sys.argv[1],sys.argv[2],str(top_customer_id[4]),str(top_customer_id[4]))
    
    command1 = cli_getter(per_customer_stats1)
    result1 = subprocess.run(command1, check=True, text=True, capture_output=True)
    data_per_customer1 = StringIO(result1.stdout)
    column_names = ["metric","value"]
    df1 = pd.read_csv(data_per_customer1, sep="\t", header=None,names=column_names)

    command2 = cli_getter(per_customer_stats2)
    result2 = subprocess.run(command2, check=True, text=True, capture_output=True)
    data_per_customer2 = StringIO(result2.stdout)
    df2 = pd.read_csv(data_per_customer2, sep="\t", header=None,names=column_names)

    command3 = cli_getter(per_customer_stats3)
    result3 = subprocess.run(command3, check=True, text=True, capture_output=True)
    data_per_customer3 = StringIO(result3.stdout)
    df3 = pd.read_csv(data_per_customer3, sep="\t", header=None,names=column_names)

    command4 = cli_getter(per_customer_stats4)
    result4 = subprocess.run(command4, check=True, text=True, capture_output=True)
    data_per_customer4 = StringIO(result4.stdout)
    df4 = pd.read_csv(data_per_customer4, sep="\t", header=None,names=column_names)

    command5 = cli_getter(per_customer_stats5)
    result5 = subprocess.run(command5, check=True, text=True, capture_output=True)
    data_per_customer5 = StringIO(result5.stdout)
    df5 = pd.read_csv(data_per_customer5, sep="\t", header=None,names=column_names)

    string1 = "{} \n\n {} \n {} \n\n {} \n {} \n\n {} \n {} \n\n".format(df.iloc[0]['customer_id_name'],df1.iloc[0]['metric'] , df1.iloc[0]['value'] ,df1.iloc[1]['metric'] , df1.iloc[1]['value'] , df1.iloc[2]['metric'] , df1.iloc[2]['value'])
    string2 = "{} \n\n {} \n {} \n\n {} \n {} \n\n {} \n {} \n\n".format(df.iloc[1]['customer_id_name'],df2.iloc[0]['metric'] , df2.iloc[0]['value'] ,df2.iloc[1]['metric'] , df2.iloc[1]['value'] , df2.iloc[2]['metric'] , df2.iloc[2]['value'])
    string3 = "{} \n\n {} \n {} \n\n {} \n {} \n\n {} \n {} \n\n".format(df.iloc[2]['customer_id_name'],df3.iloc[0]['metric'] , df3.iloc[0]['value'] ,df3.iloc[1]['metric'] , df3.iloc[1]['value'] , df3.iloc[2]['metric'] , df3.iloc[2]['value'])
    string4 = "{} \n\n {} \n {} \n\n {} \n {} \n\n {} \n {} \n\n".format(df.iloc[3]['customer_id_name'],df4.iloc[0]['metric'] , df4.iloc[0]['value'] ,df4.iloc[1]['metric'] , df4.iloc[1]['value'] , df4.iloc[2]['metric'] , df4.iloc[2]['value'])
    string5 = "{} \n\n {} \n {} \n\n {} \n {} \n\n {} \n {} \n\n".format(df.iloc[4]['customer_id_name'],df5.iloc[0]['metric'] , df5.iloc[0]['value'] ,df5.iloc[1]['metric'] , df5.iloc[1]['value'] , df5.iloc[2]['metric'] , df5.iloc[2]['value'])
    try:
        start_time = int(sys.argv[1])  # First argument is the start time (epoch)
        end_time = int(sys.argv[2])    # Second argument is the end time (epoch)
    except ValueError:
        print("Error: Start time and end time must be integers representing epoch timestamps.")
        sys.exit(1)

# Generate the output
    output = (
        "From: "
        + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        + " To: "
        + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
        + "\n\nTop Customer Drops(By highest flows): \n\n"
        + top_customer_drops
        + string1
        + string2
        + string3
        + string4
        + string5
    )


    file_name = "customer_drops.txt"

    # Open the file in write mode and save the string
    with open(file_name, "w") as file:
        file.write(output)

    print(f"Data saved to {file_name}")
    file_path = get_current_directory() + "/" + "mapnocc_ops_buddy.sh"
    process = subprocess.Popen([file_path,  sys.argv[3]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in process.stdout:
        print(str(line.strip()))
    returncode = process.wait()
    print(f"Process ended with the return code of {returncode}.")
    
except subprocess.CalledProcessError as e:
    print("Error executing query:")
    print(e.stderr)
