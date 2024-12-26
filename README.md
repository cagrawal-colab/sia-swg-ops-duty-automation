# sia-swg-ops-duty-automation
1. Download the yaml file from the prod colab cluster in the directory where the script is
2. Download kubectl for running the queries using the link - https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/
3. Run pip install -r requirements.txt
4. Run the python script using the following command - python3 ops_duty_python_script.py <start_epoch_time_local> <end_epoch_time_local> <FARSIGHT-TICKET> ### Example - python3 ops_duty_script.py 1734892200 1734978600 FARSIGHT-61479
5. Done!  :) 
