# sia-swg-ops-duty-automation
1. Download the yaml file from the prod colab cluster in the directory where the script is
2. Download kubectl for running the queries using the link - https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/
3. Change the correct directory on line 11 in the python script ops_duty_python_script.py
4. Run pip install -r requirements.txt
5. Run the python script using the following command - python ops_duty_python_script.py <start_epoch_time_local> <end_epoch_time_local>
6. Run the following command - ./mapnocc_customer_ops_duty.sh FARSIGHT-TICKET #### ./mapnocc_customer_ops_duty.sh FARSIGHT-61479
7. Done!  :) 
