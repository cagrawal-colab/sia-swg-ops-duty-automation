# sia-swg-ops-duty-automation
1. Download the yaml file from the prod colab cluster in the directory where the python script is (link - https://cloud.linode.com/kubernetes/clusters/203264/summary)
2. Run the following commands to install kubectl , if it's not there else skip the next two lines
3. curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
4. sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
5. Run pip install -r requirements.txt
6. chmod +x mapnocc_customer_ops_duty.sh
7. Run the python script using the following command - python3 ops_duty_python_script.py <start_epoch_time_local> <end_epoch_time_local> <FARSIGHT-TICKET> ### Example - python3 ops_duty_script.py 1734892200 1734978600 FARSIGHT-61479
8. Done!  :):)
