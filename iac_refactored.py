import boto3
from botocore.exceptions import ClientError
import paramiko
from scp import SCPClient
import time
import os
import subprocess
import requests

def create_ssh_client(host, user, key_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=user, key_filename=key_path)
    return ssh

def gatekeeper_script(trusted_host_ip):
    return f"""#!/bin/bash
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip
    pip3 install flask requests

    cat <<EOF > /home/ubuntu/gatekeeper.py
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

TRUSTED_HOST_URL = "http://{trusted_host_ip}:5000"

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/", methods=["POST"])
def validate_and_forward():
    # Validation basique des données
    data = request.json
    if not data or "operation" not in data or "payload" not in data:
        return jsonify({{"error": "Invalid request format"}}), 400

    # Transmettre les requêtes validées au Trusted Host
    try:
        response = requests.post(TRUSTED_HOST_URL, json=data)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({{"error": str(e)}}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
EOF

    nohup python3 /home/ubuntu/gatekeeper.py > /home/ubuntu/gatekeeper.log 2>&1 &
    """

def trusted_host_script(proxy_ip):
    return f"""#!/bin/bash
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip
    pip3 install flask requests

    cat <<EOF > /home/ubuntu/trusted_host.py
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

PROXY_URL = "http://{proxy_ip}:5000"

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/", methods=["POST"])
def process_request():
    # Transférer toutes les requêtes au Proxy
    data = request.json
    try:
        response = requests.post(PROXY_URL, json=data)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({{"error": str(e)}}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
EOF

    nohup python3 /home/ubuntu/trusted_host.py > /home/ubuntu/trusted_host.log 2>&1 &
    """


def proxy_script(manager_ip, workers_list):
    worker_urls = [f"http://{ip}:5000" for ip in workers_list]
    return f"""#!/bin/bash
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip
    pip3 install flask requests

    cat <<EOF > /home/ubuntu/proxy.py
from flask import Flask, request, jsonify
import requests
import random

app = Flask(__name__)

MANAGER_URL = "http://{manager_ip}:5000"
WORKER_URLS = {worker_urls}

def get_fastest_server():
    response_times = {{}}
    servers = WORKER_URLS
    for server in servers:
        try:
            response = requests.get(server, timeout=2)
            response_times[server] = response.elapsed.total_seconds()
        except requests.exceptions.RequestException:
            response_times[server] = float("inf")
    return min(response_times, key=response_times.get)

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/", methods=["POST"])
def process_request():
    data = request.json
    request_type = data.get("type", "").lower()
    if request_type == "write":
        try:
            response = requests.post(MANAGER_URL, json=data)
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            return jsonify({{"error": str(e)}}), 500
    elif request_type == "read":
        mode = data.get("mode", "direct_hit")
        target_url = None
        if mode == "direct_hit":
            target_url = WORKER_URLS[0]  # Direct to the first worker
        elif mode == "random":
            target_url = random.choice(WORKER_URLS)
        elif mode == "customized":
            target_url = get_fastest_server()
        if target_url:
            try:
                response = requests.post(target_url, json=data)
                return jsonify(response.json()), response.status_code
            except requests.exceptions.RequestException as e:
                return jsonify({{"error": str(e)}}), 500
    return jsonify({{"error": "Invalid request"}}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
EOF

    nohup python3 /home/ubuntu/proxy.py > /home/ubuntu/proxy.log 2>&1 &
    """


def worker_script():
    return f"""#!/bin/bash
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip mysql-server
    pip3 install flask requests mysql-connector-python

    # Configure MySQL
    sudo mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';"
    sudo mysql -u root -pyourpassword -e "CREATE DATABASE sakila;"

    # Download and import Sakila database
    wget -O /tmp/sakila-db.tar.gz https://downloads.mysql.com/docs/sakila-db.tar.gz
    tar -xvzf /tmp/sakila-db.tar.gz -C /tmp
    sudo mysql -u root -pyourpassword sakila < /tmp/sakila-db/sakila-schema.sql
    sudo mysql -u root -pyourpassword sakila < /tmp/sakila-db/sakila-data.sql

    cat <<EOF > /home/ubuntu/worker.py
from flask import Flask, request, jsonify
import mysql.connector

app = Flask(__name__)

DB_CONFIG = {{
    "host": "localhost",
    "user": "root",
    "password": "yourpassword",
    "database": "sakila"
}}

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/", methods=["POST"])
def handle_read():
    data = request.json
    if not data or "query" not in data:
        return jsonify({{"error": "Invalid request format"}}), 400

    query = data["query"]
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return jsonify(result), 200
    except mysql.connector.Error as err:
        return jsonify({{"error": str(err)}}), 500
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
EOF

    nohup python3 /home/ubuntu/worker.py > /home/ubuntu/worker.log 2>&1 &
    """


def manager_script(workers_list):
    worker_urls = [f"http://{ip}:5000" for ip in workers_list]

    return f"""#!/bin/bash
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip mysql-server
    pip3 install flask requests mysql-connector-python

    # Configure MySQL
    sudo mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';"
    sudo mysql -u root -pyourpassword -e "CREATE DATABASE sakila;"

    # Download and import Sakila database
    wget -O /tmp/sakila-db.tar.gz https://downloads.mysql.com/docs/sakila-db.tar.gz
    tar -xvzf /tmp/sakila-db.tar.gz -C /tmp
    sudo mysql -u root -pyourpassword sakila < /tmp/sakila-db/sakila-schema.sql
    sudo mysql -u root -pyourpassword sakila < /tmp/sakila-db/sakila-data.sql

    cat <<EOF > /home/ubuntu/manager.py
from flask import Flask, request, jsonify
import mysql.connector
import requests
import random

app = Flask(__name__)

DB_CONFIG = {{
    "host": "localhost",
    "user": "root",
    "password": "yourpassword",
    "database": "sakila"
}}

WORKER_URLS = {worker_urls}

def get_fastest_worker():
    response_times = {{}}
    for worker in WORKER_URLS:
        try:
            response = requests.get(worker + "/health", timeout=2)
            response_times[worker] = response.elapsed.total_seconds()
        except requests.exceptions.RequestException:
            response_times[worker] = float('inf')
    return min(response_times, key=response_times.get)

@app.route("/", methods=["POST"])
def handle_request():
    data = request.json
    if not data or "query" not in data or "type" not in data:
        return jsonify({{"error": "Invalid request format"}}), 400

    query = data["query"]
    request_type = data["type"].lower()
    mode = data.get("mode", "direct_hit").lower()

    if request_type == "write":
        # Handle write operations locally
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return jsonify({{"status": "success"}}), 200
        except mysql.connector.Error as err:
            return jsonify({{"error": str(err)}}), 500
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    elif request_type == "read":
        # Handle read operations by communicating with workers
        if mode == "direct_hit":
            target_worker = WORKER_URLS[0]
        elif mode == "random":
            target_worker = random.choice(WORKER_URLS)
        elif mode == "customized":
            target_worker = get_fastest_worker()
        else:
            return jsonify({{"error": "Invalid mode"}}), 400

        try:
            response = requests.post(target_worker, json={{"query": query}})
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            return jsonify({{"error": str(e)}}), 500
    else:
        return jsonify({{"error": "Invalid request type"}}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
EOF

    nohup python3 /home/ubuntu/manager.py > /home/ubuntu/manager.log 2>&1 &
    """

class EC2Manager:

    def __init__(self):

        self.key_name = "temp_key_pair"

        # Clients and resources
        self.ec2_client = boto3.client("ec2", region_name="us-east-1")
        self.ec2_resource = boto3.resource("ec2", region_name="us-east-1")

        # Ids
        self.vpc_id = self.ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]
        self.ami_id = self._get_latest_ubuntu_ami()

        # Security groups
        self.security_group_mysql = self.ec2_client.create_security_group(
            Description="Common security group",
            GroupName="common_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.security_group_gatekeeper = self.ec2_client.create_security_group(
            Description="Gatekeeper security group",
            GroupName="gatekeeper_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.security_group_trusted_host = self.ec2_client.create_security_group(
            Description="Trusted Host security group",
            GroupName="trusted_host_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.security_group_proxy = self.ec2_client.create_security_group(
            Description="Proxy security group",
            GroupName="proxy_sg",
            VpcId=self.vpc_id,
        )["GroupId"]
        

        self._add_inboud_rule_security_group()

        # Initialized later
        self.mysql_instances = None
        self.proxy_instance = None
        self.gatekeeper_instance = None
        self.trusted_host_instance = None

    def create_key_pair(self):
        response = self.ec2_client.create_key_pair(KeyName=self.key_name)
        private_key = response['KeyMaterial']
        with open(f'{self.key_name}.pem', 'w') as file:
            file.write(private_key)

    def launch_instances(self):
        self.mysql_instances = []

        for i in range(2):  # Lancer 2 instances worker
            worker_instance = self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.micro",
                KeyName=self.key_name,
                SecurityGroupIds=[self.security_group_mysql],
                MinCount=1,
                MaxCount=1,
                #UserData=worker_script()

            )[0]  # Accéder à l'unique instance créée
            self.mysql_instances.append(worker_instance)
        # Lancer l'instance manager
        self.manager_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.micro",
            KeyName=self.key_name,
            SecurityGroupIds=[self.security_group_mysql],
            MinCount=1,
            MaxCount=1,
            #UserData=manager_script(self.mysql_instances[0:1])
        )[0]  # Accéder à l'unique instance créée
        self.mysql_instances.append(self.manager_instance)

        # Lancer l'instance Proxy
        self.proxy_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_proxy],
            KeyName=self.key_name,
            UserData=proxy_script(self.manager_instance.public_ip_address, [instance.public_ip_address for instance in self.mysql_instances])
        )[0]  # Accéder à l'unique instance créée

        # Lancer l'instance Trusted Host
        self.trusted_host_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_trusted_host],
            KeyName=self.key_name,
            UserData=trusted_host_script(self.proxy_instance.public_ip_address)
        )[0]  # Accéder à l'unique instance créée


        # Lancer l'instance Gatekeeper
        self.gatekeeper_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_gatekeeper],
            KeyName=self.key_name,
            UserData=gatekeeper_script(self.trusted_host_instance.public_ip_address)
        )[0]  # Accéder à l'unique instance créée

        return self.mysql_instances + [self.proxy_instance] + [self.gatekeeper_instance] + [self.trusted_host_instance]


    def run_sql_sakila_install(self):
        commands = [
            # Mise à jour et installation des paquets
            "sudo apt-get update",
            "sudo apt-get upgrade -y",
            "sudo apt-get install -y mysql-server",
            "sudo apt-get install -y sysbench",
            "sudo apt-get install python3-pip -y",
            "pip3 install flask requests",
            
            # Téléchargement et extraction de la base Sakila
            "wget -O /tmp/sakila-db.tar.gz https://downloads.mysql.com/docs/sakila-db.tar.gz",
            "tar -xvzf /tmp/sakila-db.tar.gz -C /tmp",
            
            # Importation de la base Sakila dans MySQL
            "sudo mysql -u root -e 'SOURCE /tmp/sakila-db/sakila-schema.sql;'",
            "sudo mysql -u root -e 'SOURCE /tmp/sakila-db/sakila-data.sql;'",
            
            # Configuration du mot de passe root MySQL
            "sudo mysql -e 'ALTER USER \"root\"@\"localhost\" IDENTIFIED WITH \"mysql_native_password\" BY \"yourpassword\";'",
            
            # Préparation pour Sysbench
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user=root --mysql-password='yourpassword' prepare",
            
            # Exécution du benchmark Sysbench
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user=root --mysql-password='yourpassword' run",
            
            # Nettoyage après le benchmark
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user=root --mysql-password='yourpassword' cleanup"
    ]

        try:
            # Connect to the instance
            ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")
            for instance in self.mysql_instances:
                ssh_client = create_ssh_client(instance.public_ip_address, "ubuntu", ssh_key_path)     
            
            # Run the commands
            for command in commands:
                print(f"Executing command: {command}")
                stdin, stdout, stderr = ssh_client.exec_command(command)
                
                # Process output in real-time
                for line in iter(stdout.readline, ""):
                    print(line, end="")  # Print each line from stdout
                error_output = stderr.read().decode()  # Capture any error output

                # Wait for command to complete
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    print(f"Command '{command}' failed with exit status {exit_status}. Error:\n{error_output}")
            
            time.sleep(2)

        finally:
            ssh_client.close()

    def configure_proxy(self, proxy_instance):
        commands = [
            "sudo apt-get update",
            "sudo apt-get install python3-pip -y",
            "pip3 install flask requests",
        ]

        # Exécuter les commandes sur l'instance Proxy
        try:
            # Connect to the instance
            ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")
            ssh_client = create_ssh_client(proxy_instance.public_ip_address, "ubuntu", ssh_key_path)

            # Run the commands
            for command in commands:
                print(f"Executing command: {command}")
                stdin, stdout, stderr = ssh_client.exec_command(command)
                
                # Process output in real-time
                for line in iter(stdout.readline, ""):
                    print(line, end="")  # Print each line from stdout
                error_output = stderr.read().decode()  # Capture any error output

                # Wait for command to complete
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    print(f"Command '{command}' failed with exit status {exit_status}. Error:\n{error_output}")
            
            time.sleep(2)

        finally:
            ssh_client.close()

    def configure_gatekeeper(self, gatekeeper_instance):
        commands = [
            "sudo apt-get update",
            "sudo apt-get install python3-pip -y",
            "pip3 install flask requests",
        ]

        # Exécuter les commandes sur l'instance Gatekeeper
        try:
            # Connect to the instance
            ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")
            ssh_client = create_ssh_client(gatekeeper_instance.public_ip_address, "ubuntu", ssh_key_path)

            # Run the commands
            for command in commands:
                print(f"Executing command: {command}")
                stdin, stdout, stderr = ssh_client.exec_command(command)
                
                # Process output in real-time
                for line in iter(stdout.readline, ""):
                    print(line, end="")  # Print each line from stdout
                error_output = stderr.read().decode()  # Capture any error output

                # Wait for command to complete
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    print(f"Command '{command}' failed with exit status {exit_status}. Error:\n{error_output}")
            
            time.sleep(2)
        
        finally:
            ssh_client.close()

    def configure_trusted_host(self, trusted_host_instance):
        commands = [
            "sudo apt-get update",
            "sudo apt-get install python3-pip -y",
            "pip3 install flask requests",
        ]

        # Exécuter les commandes sur l'instance Gatekeeper
        try:
            # Connect to the instance
            ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")
            ssh_client = create_ssh_client(trusted_host_instance.public_ip_address, "ubuntu", ssh_key_path)

            # Run the commands
            for command in commands:
                print(f"Executing command: {command}")
                stdin, stdout, stderr = ssh_client.exec_command(command)
                
                # Process output in real-time
                for line in iter(stdout.readline, ""):
                    print(line, end="")  # Print each line from stdout
                error_output = stderr.read().decode()  # Capture any error output

                # Wait for command to complete
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    print(f"Command '{command}' failed with exit status {exit_status}. Error:\n{error_output}")
            
            time.sleep(2)
        
        finally:
            ssh_client.close()

    def _add_inboud_rule_security_group(self):
        """
        Add inbound rules to the security group to allow SSH and application port traffic.
        """
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.security_group_mysql,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,  # SSH
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },

                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,  # MySQL
                    "ToPort": 5000,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
            ],
        #TODO: Ajust other inbound rules for the gatekeeper and trusted host ans proxy and mysql_instances
        )

        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.security_group_proxy,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,  # SSH
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },

                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,  # Proxy
                    "ToPort": 5000,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
            ],
        )

        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.security_group_trusted_host,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,  # SSH
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },

                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,  # Trusted Host
                    "ToPort": 5000,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
            ],
        )

        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.security_group_gatekeeper,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,  # SSH
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },

                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,  # Gatekeeper
                    "ToPort": 5000,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },                
            ],
        )

    def _get_latest_ubuntu_ami(self):
        """
        Get the latest Ubuntu AMI ID.
        """
        response = self.ec2_client.describe_images(
            Filters=[
                {
                    "Name": "name",
                    "Values": [
                        "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"
                    ],
                },
                {"Name": "virtualization-type", "Values": ["hvm"]},
                {"Name": "architecture", "Values": ["x86_64"]},
            ],
            Owners=["099720109477"],  # Canonical
        )
        images = response["Images"]
        images.sort(key=lambda x: x["CreationDate"], reverse=True)
        return images[0]["ImageId"]
    
    def cleanup(self):
        """
        Delete the target groups, terminate all instances and delete the security group.

        Raises:
            ClientError: When an error occurs when deleting resources.
        """
        try:

            # Terminate EC2 mysql instances
            instance_ids = [
                instance.id
                for instance in self.mysql_instances
            ]
            if instance_ids:
                self.ec2_client.terminate_instances(InstanceIds=instance_ids)
                print(f"Termination of instances {instance_ids} initiated.")

                # Wait for termination to complete (optional but recommended)
                waiter = self.ec2_client.get_waiter("instance_terminated")
                waiter.wait(InstanceIds=instance_ids)
                print("Instances terminated.")

            # Terminate proxy instance
            if self.proxy_instance:
                self.ec2_client.terminate_instances(InstanceIds=[self.proxy_instance.id])
                print(f"Termination of proxy instance {self.proxy_instance.id} initiated.")

                # Wait for termination to complete (optional but recommended)
                waiter = self.ec2_client.get_waiter("instance_terminated")
                waiter.wait(InstanceIds=[self.proxy_instance.id])
                print("Proxy instance terminated.")

            # Terminate gatekeeper instance
            if self.gatekeeper_instance:
                self.ec2_client.terminate_instances(InstanceIds=[self.gatekeeper_instance.id])
                print(f"Termination of instance {self.gatekeeper_instance.id} initiated.")

                # Wait for termination to complete (optional but recommended)
                waiter = self.ec2_client.get_waiter("instance_terminated")
                waiter.wait(InstanceIds=[self.gatekeeper_instance.id])
                print("Gatekeeper instance terminated.")

            # Terminate trusted host instance
            if self.trusted_host_instance:
                self.ec2_client.terminate_instances(InstanceIds=[self.trusted_host_instance.id])
                print(f"Termination of instance {self.trusted_host_instance.id} initiated.")

                # Wait for termination to complete (optional but recommended)
                waiter = self.ec2_client.get_waiter("instance_terminated")
                waiter.wait(InstanceIds=[self.trusted_host_instance.id])
                print("Trusted host instance terminated.")

            # Delete security groups
            self.ec2_client.delete_security_group(GroupId=self.security_group_mysql)
            print(f"Security group {self.security_group_mysql} deleted.")

            self.ec2_client.delete_security_group(GroupId=self.security_group_proxy)
            print(f"Security group {self.security_group_proxy} deleted.")

            self.ec2_client.delete_security_group(GroupId=self.security_group_trusted_host)
            print(f"Security group {self.security_group_trusted_host} deleted.")

            self.ec2_client.delete_security_group(GroupId=self.security_group_gatekeeper)
            print(f"Security group {self.security_group_gatekeeper} deleted.")

            # Delete key pair
            self.ec2_client.delete_key_pair(KeyName=self.key_name)

        except ClientError as e:
            print(f"An error occurred: {e}")

    #TODO: Send 2000 requests to the gate
    def request_to_gate(self):
        """
        Send 1000 read and 100 write requests to the Gatekeeper.
        Test all three routing modes: direct_hit, random, and customized.
        """

        gatekeeper_url = f"http://{self.gatekeeper_instance.public_ip_address}:5000"  # Assuming port 5000 for the Gatekeeper API
        print(f"Gatekeeper URL: {gatekeeper_url}")
        #request_types = ["read"] * 1000 + ["write"] * 1000  # 1000 reads and 100 writes
        request_types = ["read"] * 10
        modes = ["direct_hit", "random", "customized"]  # Modes to test
        results = {mode: {"read_success": 0, "read_failures": 0, "write_success": 0, "write_failures": 0} for mode in modes}

        for mode in modes:
            for request_type in request_types:
                payload = {
                    "type": request_type,
                    "mode": mode,
                    "query": "SELECT * FROM actor LIMIT 10;" if request_type == "read" else "INSERT INTO actor (first_name, last_name) VALUES ('Test', 'User');"
                }
                try:
                    response = requests.post(gatekeeper_url, json=payload, timeout=5)
                    if response.status_code == 200:
                        results[mode][f"{request_type}_success"] += 1
                    else:
                        results[mode][f"{request_type}_failures"] += 1
                except requests.exceptions.RequestException as e:
                    print(f"Request failed in mode {mode} ({request_type}): {e}")
                    results[mode][f"{request_type}_failures"] += 1

        print(f"Request results by mode: {results}")
        return results

    #Print instances public IPs
    def print_instances_public_ips(self):
        print("MySQL Instances:")
        for instance in self.mysql_instances:
            print(f"  - {instance.public_ip_address}")

        print(f"Proxy Instance: {self.proxy_instance.public_ip_address}")
        print(f"Gatekeeper Instance: {self.gatekeeper_instance.public_ip_address}")
        print(f"Trusted Host Instance: {self.trusted_host_instance.public_ip_address}")

    #TODO: Implement the benchmarks method to mesure the response time of the mysql instances
    def benchmarks(self):
        """
        Measure response times of MySQL cluster for 100 read and 10 write requests per mode.
        Test all three routing modes: direct_hit, random, and customized.
        """
        import time
        import requests

        proxy_url = f"http://{self.proxy_instance.public_ip_address}:5000"  # Assuming port 5000 for the Proxy API
        modes = ["direct_hit", "random", "customized"]
        queries = {
            "read": "SELECT * FROM actor LIMIT 10;",
            "write": "INSERT INTO actor (first_name, last_name) VALUES ('Benchmark', 'Test');"
        }

        results = {mode: {"read_times": [], "write_times": []} for mode in modes}

        for mode in modes:
            for query_type, query in queries.items():
                num_requests = 100 if query_type == "read" else 10  # 100 reads, 10 writes per mode
                for _ in range(num_requests):
                    start_time = time.time()
                    try:
                        response = requests.post(proxy_url, json={"query": query, "mode": mode}, timeout=5)
                        if response.status_code == 200:
                            elapsed_time = time.time() - start_time
                            results[mode][f"{query_type}_times"].append(elapsed_time)
                        else:
                            print(f"Request failed in mode {mode} ({query_type}): {response.text}")
                    except requests.exceptions.RequestException as e:
                        print(f"Benchmark request failed in mode {mode} ({query_type}): {e}")

        # Calculate average response times
        summary = {}
        for mode in results:
            summary[mode] = {
                "average_read_time": sum(results[mode]["read_times"]) / len(results[mode]["read_times"]) if results[mode]["read_times"] else float('inf'),
                "average_write_time": sum(results[mode]["write_times"]) / len(results[mode]["write_times"]) if results[mode]["write_times"] else float('inf')
            }

        print("Benchmark results summary:")
        for mode, stats in summary.items():
            print(f"Mode: {mode}")
            print(f"  Average Read Time: {stats['average_read_time']:.4f} seconds")
            print(f"  Average Write Time: {stats['average_write_time']:.4f} seconds")

        return summary


# Main 

# Créer une clé pour l'accès SSH
ec2_manager = EC2Manager()
ec2_manager.create_key_pair()
time.sleep(5)

# Lancer les instances MySQL
instances = ec2_manager.launch_instances()

# Attendre que les instances MySQL soient en cours d'exécution
print("Waiting for instances to be running...")
for instance in instances:
    instance.wait_until_running()
    instance.reload()
print("Instances are running.")

time.sleep(5)

ec2_manager.run_sql_sakila_install()
ec2_manager.configure_proxy(ec2_manager.proxy_instance)
ec2_manager.configure_gatekeeper(ec2_manager.gatekeeper_instance)
ec2_manager.configure_trusted_host(ec2_manager.trusted_host_instance)

# Afficher les adresses IP publiques des instances
ec2_manager.print_instances_public_ips()

#TODO: Implement the benchmarks method by sending 2000 requests to the gate
# Envoyer des requêtes au Gatekeeper
# print("Sending requests to the Gatekeeper...")
# gate_results = ec2_manager.request_to_gate()
# print(f"Gatekeeper test results: {gate_results}")

# Lancer les benchmarks
# print("Running benchmarks...")
# benchmark_results = ec2_manager.benchmarks()
# print(f"Benchmark results: {benchmark_results}")

# Nettoyage après l'exécution
input("Press Enter to terminate and cleanup all resources...")
ec2_manager.cleanup()
print("Cleanup complete.")