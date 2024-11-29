import boto3
from botocore.exceptions import ClientError
import paramiko
from scp import SCPClient
import time
import os
import subprocess

# Function to create an SSH client
def create_ssh_client(host, user, key_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=user, key_filename=key_path)
    return ssh

def gatekeeper_script(trusted_host_ip):
        return f"""
    #!/bin/bash

    # Mise à jour et installation des paquets nécessaires
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip ufw
    pip3 install flask requests

    # Configuration du pare-feu
    sudo ufw allow 80
    sudo ufw deny 22
    sudo ufw enable

    # Création du script Flask Gatekeeper
    echo '
    from flask import Flask, request, jsonify
    import requests

    app = Flask(__name__)

    TRUSTED_HOST_URL = "http://{trusted_host_ip}:5000"

    @app.route("/", methods=["POST"])
    def process_request():
        # Valider les requêtes entrantes ici (ajoutez vos propres vérifications)
        data = request.json
        return requests.post(TRUSTED_HOST_URL, json=data).json()

    if __name__ == "__main__":
        app.run(host="0.0.0.0", port=80)
    ' > /home/ubuntu/gatekeeper.py

    # Démarrer le serveur Flask
    nohup python3 /home/ubuntu/gatekeeper.py &
    """


def trusted_host_script(proxy_ip):
        return f"""
    #!/bin/bash

    # Mise à jour et installation des paquets nécessaires
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip ufw
    pip3 install flask requests

    # Configuration du pare-feu
    sudo ufw allow from {proxy_ip}
    sudo ufw deny 22
    sudo ufw enable

    # Création du script Flask Trusted Host
    echo '
    from flask import Flask, request, jsonify
    import requests

    app = Flask(__name__)

    PROXY_URL = "http://{proxy_ip}:5000"

    @app.route("/", methods=["POST"])
    def process_request():
        # Transférer toutes les requêtes au Proxy
        data = request.json
        return requests.post(PROXY_URL, json=data).json()

    if __name__ == "__main__":
        app.run(host="0.0.0.0", port=5000)
    ' > /home/ubuntu/trusted_host.py

    # Démarrer le serveur Flask
    nohup python3 /home/ubuntu/trusted_host.py &
    """


def proxy_script(manager_ip, workers_list):
        return f"""
    #!/bin/bash

    # Mise à jour et installation des paquets nécessaires
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip
    pip3 install flask requests

    # Création du script Flask Proxy
    echo '
    from flask import Flask, request, jsonify
    import requests
    import random

    app = Flask(__name__)

    MANAGER_URL = "http://{manager_ip}:3306"
    WORKER_URLS = {workers_list}

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

    @app.route("/", methods=["POST"])
    def process_request():
        data = request.json
        request_type = data.get("type", "").lower()
        if request_type == "write":
            return requests.post(MANAGER_URL, json=data).json()
        elif request_type == "read":
            mode = data.get("mode", "direct_hit")
            if mode == "random":
                return requests.post(random.choice(WORKER_URLS), json=data).json()
            elif mode == "customized":
                return requests.post(get_fastest_server(), json=data).json()
        return {{"error": "Invalid request"}}, 400

    if __name__ == "__main__":
        app.run(host="0.0.0.0", port=5000)
    ' > /home/ubuntu/proxy.py

    # Démarrer le serveur Flask
    nohup python3 /home/ubuntu/proxy.py &
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

        self.security_group_id = self.ec2_client.create_security_group(
            Description="Common security group",
            GroupName="common_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self._add_inboud_rule_security_group()

        # Initialized later
        self.mysql_instances = None
        self.proxy_instance = None
        self.gatekeeper_instances = None

    def create_key_pair(self):
        response = self.ec2_client.create_key_pair(KeyName=self.key_name)
        private_key = response['KeyMaterial']
        with open(f'{self.key_name}.pem', 'w') as file:
            file.write(private_key)

    def launch_mysql_instances(self):
        """
        Launch instances
        """
        self.mysql_instances = []
        for i in range(3):
            instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.micro",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_id],
            KeyName=self.key_name,
            #UserData=sql_script,
            BlockDeviceMappings=[
                {
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'VolumeSize': 16,
                'VolumeType': 'gp3',
                'DeleteOnTermination': True
                }
            }
        ]
        )[0]
            self.mysql_instances.append(instance)
        return self.mysql_instances
    
    def launch_proxy_instance(self, manager_ip, worker_ips):
        print("Launching proxy instance...")
        self.proxy_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_id],
            KeyName=self.key_name,
            UserData=proxy_script(manager_ip, worker_ips),
    )[0]

        self.proxy_instance.wait_until_running()
        self.proxy_instance.reload()  # Actualise les informations après le démarrage
        print(f"Proxy instance launched: {self.proxy_instance.public_ip_address}")
        return self.proxy_instance

    def launch_gatekeeper_instances(self, trusted_host_ip, proxy_ip):
        self.gatekeeper_instances = []
        self.gatekeeper_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            UserData=gatekeeper_script(trusted_host_ip),
            SecurityGroupIds=[self.security_group_id],
            KeyName=self.key_name,
        )[0]
        self.gatekeeper_instances.append(instance)

        self.trusted_host_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            UserData=trusted_host_script(proxy_ip),
            SecurityGroupIds=[self.security_group_id],
            KeyName=self.key_name,
        )[0]
        self.gatekeeper_instances.append(instance)

        return self.gatekeeper_instance, self.trusted_host_instance
    
    def run_sql_sakila_install(self):
        commands = [
            # Mise à jour et installation des paquets
            "sudo apt-get update",
            "sudo apt-get upgrade -y",
            "sudo apt-get install -y mysql-server",
            "sudo apt-get install -y sysbench",
            
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

            # scp = SCPClient(ssh_client.get_transport())

            # # Download the output files for the pg4300.txt word count
            # scp.get("~/pg4300_outputs/linux_output.txt", "./results/linux_output.txt")
            # scp.get("~/pg4300_outputs/hadoop_output.txt", "./results/hadoop_output.txt")
            # scp.get("~/pg4300_outputs/spark_output.txt", "./results/spark_output.txt")
            # print("Benchmark results downloaded to linux_output.txt, hadoop_output.txt, and spark_output.txt")


            # # Run the benchmark script
            # scp.put("./benchmark_wordcount.py", "~/benchmark_wordcount.py")
            # stdin, stdout, stderr = ssh_client.exec_command("python3 benchmark_wordcount.py")
            # stdout.channel.recv_exit_status()
            
            # # Download the benchmark results
            # scp.get("~/benchmark_results.csv", "./results/benchmark_results.csv")
            # print("Benchmark results downloaded to benchmark_results.csv")

            # # Plot the data
            # scp.put("./plot.py", "~/plot.py")
            # stdin, stdout, stderr = ssh_client.exec_command("python3 plot.py")
            # stdout.channel.recv_exit_status()

            # #Download the plots
            # scp.get("~/linux_hadoop_comparison.png", "./results/linux_hadoop_comparison.png")
            # scp.get("~/hadoop_spark_comparison.png", "./results/hadoop_spark_comparison.png")

            # # Run the friends script
            # scp.put("./dataset.tsv", "~/dataset.tsv")
            # scp.put("./friends.py", "~/friends.py")
            # stdin, stdout, stderr = ssh_client.exec_command("python3 friends.py")
            # stdout.channel.recv_exit_status()
            
            # # Download the friends results
            # scp.get("~/recommendations.tsv", "./results/recommendations.tsv")
            # print("Recommendations downloaded to recommendations.tsv")

            # scp.close()
    
        finally:
            ssh_client.close()

    def get_instance_ips(self):
        manager_ip = self.mysql_instances[0].public_ip_address
        worker_ips = [instance.public_ip_address for instance in self.mysql_instances[1:]]
        return manager_ip, worker_ips

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

    def reconfigure_gatekeeper(self, gatekeeper_instance, trusted_host_ip):
        """
        Reconfigure the Gatekeeper instance with the actual IP of the Trusted Host.
        """
        print(f"Reconfiguring Gatekeeper with Trusted Host IP: {trusted_host_ip}")
        user_data = gatekeeper_script(trusted_host_ip)

        # Connect to the Gatekeeper instance and update the script
        ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")
        ssh_client = create_ssh_client(gatekeeper_instance.public_ip_address, "ubuntu", ssh_key_path)
        try:
            stdin, stdout, stderr = ssh_client.exec_command(f'echo "{user_data}" > ~/gatekeeper.py && nohup python3 ~/gatekeeper.py &')
            print(stdout.read().decode())
            print("Gatekeeper successfully reconfigured.")
        finally:
            ssh_client.close()

    def cleanup(self):
        """
        Delete the target groups, terminate all instances and delete the security group.

        Raises:
            ClientError: When an error occurs when deleting resources.
        """
        try:

            # Terminate EC2 instances
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

            # Terminate gatekeeper and trusted host instances
            # Terminate EC2 instances
            instance_ids = [
                instance.id
                for instance in self.gatekeeper_instances
            ]
            if instance_ids:
                self.ec2_client.terminate_instances(InstanceIds=instance_ids)
                print(f"Termination of instances {instance_ids} initiated.")

                # Wait for termination to complete (optional but recommended)
                waiter = self.ec2_client.get_waiter("instance_terminated")
                waiter.wait(InstanceIds=instance_ids)
                print("Instances terminated.")

            # Delete security group
            self.ec2_client.delete_security_group(GroupId=self.security_group_id)
            print(f"Security group {self.security_group_id} deleted.")

            # Delete key pair
            self.ec2_client.delete_key_pair(KeyName=self.key_name)

        except ClientError as e:
            print(f"An error occurred: {e}")

    def _add_inboud_rule_security_group(self):
        """
        Add inbound rules to the security group to allow SSH and application port traffic.
        """
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,  # SSH
                    "ToPort": 22,
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


# Main

# Créer une clé pour l'accès SSH
ec2_manager = EC2Manager()
ec2_manager.create_key_pair()
time.sleep(5)

# Lancer les instances MySQL
mysql_instances = ec2_manager.launch_mysql_instances()

# Attendre que les instances MySQL soient en cours d'exécution
print("Waiting for MySQL instances to be running...")
for instance in mysql_instances:
    instance.wait_until_running()
    instance.reload()
print("MySQL instances are running.")

# Obtenir les adresses IP des instances MySQL
manager_ip, worker_ips = ec2_manager.get_instance_ips()

# Lancer l'instance Proxy
print("Launching Proxy instance...")
proxy_instance = ec2_manager.launch_proxy_instance(manager_ip, worker_ips)
proxy_ip = proxy_instance.public_ip_address
print(f"Proxy instance is running with IP: {proxy_ip}")

# Lancer les instances Gatekeeper et Trusted Host
print("Launching Gatekeeper and Trusted Host instances...")
gatekeeper_instance, trusted_host_instance = ec2_manager.launch_gatekeeper_instances(
    trusted_host_ip="PLACEHOLDER",  # Initialement un placeholder
    proxy_ip=proxy_ip,
)

# Attendre que les instances soient en cours d'exécution
print("Waiting for Gatekeeper and Trusted Host instances to be running...")
gatekeeper_instance.wait_until_running()
trusted_host_instance.wait_until_running()
gatekeeper_instance.reload()
trusted_host_instance.reload()

# Mettre à jour le Gatekeeper avec l'IP réelle du Trusted Host
trusted_host_ip = trusted_host_instance.public_ip_address
print(f"Trusted Host instance is running with IP: {trusted_host_ip}")
ec2_manager.reconfigure_gatekeeper(gatekeeper_instance, trusted_host_ip)

print("Gatekeeper and Trusted Host instances are configured and running.")

# Exécuter l'installation de la base de données Sakila sur MySQL
print("Installing Sakila database...")
ec2_manager.run_sql_sakila_install()

# Démarrer les requêtes de test
print("Starting test requests...")

try:
    # Appeler requests.py comme un sous-processus
    subprocess.run(["python3", "requests.py"], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error while running requests.py: {e}")


# Nettoyage après l'exécution
input("Press Enter to terminate and cleanup all resources...")
ec2_manager.cleanup()
print("Cleanup complete.")

