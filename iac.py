import boto3
from botocore.exceptions import ClientError
import paramiko
from scp import SCPClient
import time
import os
import subprocess
import requests
import json

def create_ssh_client(host, user, key_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=user, key_filename=key_path)
    return ssh

class EC2Manager:

    def __init__(self):

        self.key_name = "temp_key_pair"
        self.ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")

        # Clients and resources
        self.ec2_client = boto3.client("ec2", region_name="us-east-1")
        self.ec2_resource = boto3.resource("ec2", region_name="us-east-1")

        # Ids
        self.vpc_id = self.ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]
        self.ami_id = self._get_latest_ubuntu_ami()

        # Security groups
        self.security_group_mysql = self.ec2_client.create_security_group(
            Description="MySQL instances security group",
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

        # Initialized later
        self.worker_instances = None
        self.manager_instance = None
        self.proxy_instance = None
        self.gatekeeper_instance = None
        self.trusted_host_instance = None

    def create_key_pair(self):
        response = self.ec2_client.create_key_pair(KeyName=self.key_name)
        private_key = response['KeyMaterial']
        with open(f'{self.key_name}.pem', 'w') as file:
            file.write(private_key)

    def launch_instances(self):
        self.worker_instances = []

        for i in range(2):  # Lancer 2 instances worker
            worker_instance = self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.micro",
                KeyName=self.key_name,
                SecurityGroupIds=[self.security_group_mysql],
                MinCount=1,
                MaxCount=1,

            )[0]
            self.worker_instances.append(worker_instance)

        # Launch manager instance
        self.manager_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.micro",
            KeyName=self.key_name,
            SecurityGroupIds=[self.security_group_mysql],
            MinCount=1,
            MaxCount=1,
        )[0]

        # Launch proxy instance
        self.proxy_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_proxy],
            KeyName=self.key_name,
        )[0]

        # Lancer l'instance Trusted Host
        self.trusted_host_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_trusted_host],
            KeyName=self.key_name,
        )[0]


        # Launch gatekeeper instance
        self.gatekeeper_instance = self.ec2_resource.create_instances(
            ImageId=self.ami_id,
            InstanceType="t2.large",
            MinCount=1,
            MaxCount=1,
            SecurityGroupIds=[self.security_group_gatekeeper],
            KeyName=self.key_name,
        )[0]

        return self.worker_instances + [self.manager_instance] + [self.proxy_instance] + [self.gatekeeper_instance] + [self.trusted_host_instance]


    def run_sql_sakila_install(self):
        commands = [
            # Update and dependencies installation
            "sudo apt-get update",
            "sudo apt-get install -y mysql-server wget sysbench python3-pip",
            "sudo pip3 install flask mysql-connector-python requests",

            # Start MySQL
            "sudo systemctl start mysql",
            "sudo systemctl enable mysql",

            # Password root MySQL configuration
            'sudo mysql -e \'ALTER USER "root"@"localhost" IDENTIFIED WITH "mysql_native_password" BY "password";\'',
            
            # Download Sakila database
            "wget -O /tmp/sakila-db.tar.gz https://downloads.mysql.com/docs/sakila-db.tar.gz",
            "tar -xvzf /tmp/sakila-db.tar.gz -C /tmp",

            # Import Sakila in MySQL
            "sudo mysql -u root --password=password < /tmp/sakila-db/sakila-schema.sql",
            "sudo mysql -u root --password=password < /tmp/sakila-db/sakila-data.sql",
            
            # Prepare for Sysbench
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user=root --mysql-password='password' prepare",
            
            # Execution of benchmark Sysbench
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user=root --mysql-password='password' run",
            
            # Cleaning after benchmark
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user=root --mysql-password='password' cleanup"
    ]

        try:
            # Connect to the instance

            for instance in self.worker_instances + [self.manager_instance]:
                ssh_client = create_ssh_client(instance.public_ip_address, "ubuntu", self.ssh_key_path)     
            
                # Run the commands
                for command in commands:
                    print(f"Executing command: {command} on instance {instance.public_ip_address}")
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

    # Configure proxy instance, gatekeeper instance and trusted host instance
    def configure_instance(self, instance):
        commands = [
            "sudo apt-get update",
            "sudo apt-get install python3-pip -y",
            "pip3 install flask requests",
        ]

        # Execute the commands on the proxy instance
        try:
            # Connect to the instance
            ssh_client = create_ssh_client(instance.public_ip_address, "ubuntu", self.ssh_key_path)

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
        try:
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
                        "FromPort": 5000,  # From Proxy and manager
                        "ToPort": 5000,
                        "IpRanges": [{"CidrIp": f"{self.proxy_instance.public_ip_address}/32"},
                                     
                                    {"CidrIp": f"{self.manager_instance.public_ip_address}/32"}],
                    },
                ],
            )
            print("Inbound rule added to the security group for port 5000.")
        except Exception as e:
            print(f"An error occurred while adding inbound rule: {e}")

        try:
            self.ec2_client.authorize_security_group_ingress(
                GroupId=self.security_group_proxy,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 22,  # SSH
                        "ToPort": 22,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}], # SSH
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 5000,  # From trusted host
                        "ToPort": 5000,
                        "IpRanges": [{"CidrIp": f"{self.trusted_host_instance.public_ip_address}/32"}], 
                    },
                ],
            )
            print("Inbound rule added to the security group for port 5000.")
        except Exception as e:
            print(f"An error occurred while adding inbound rule: {e}")

        try:
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
                        "FromPort": 5000,  # From Gatekeeper
                        "ToPort": 5000,
                        "IpRanges": [{"CidrIp": f"{self.gatekeeper_instance.public_ip_address}/32"}],
                    },
                ],
            )
            print("Inbound rule added to the security group for port 5000.")
        except Exception as e:
            print(f"An error occurred while adding inbound rule: {e}")

        try:
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
                        "FromPort": 5000,  # From any IP
                        "ToPort": 5000,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    },                
                ],
            )
            print("Inbound rule added to the security group for port 5000.")
        except Exception as e:
            print(f"An error occurred while adding inbound rule: {e}")
            

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
                for instance in self.worker_instances + [self.manager_instance]
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

    #Get the public IPs of the instances and write a .json file
    def export_instances_public_ips(self):
        instances_ips = {
            "worker_ips": [instance.public_ip_address for instance in self.worker_instances],
            "manager_ip": self.manager_instance.public_ip_address,
            "proxy_ip": self.proxy_instance.public_ip_address,
            "gatekeeper_ip": self.gatekeeper_instance.public_ip_address,
            "trusted_host_ip": self.trusted_host_instance.public_ip_address
        }

        with open("instances_ips.json", "w") as file:
            json.dump(instances_ips, file)

    
    #Put the script in the instances
    def upload_flask_apps_to_instances(self) -> None:
        """Upload the corresponding Flask server scripts to each instance."""
        # Mapping of instances to their script filenames
        instance_script_mapping = [
            (self.manager_instance.public_ip_address, "scripts/manager_script.py", "manager_script.py"),
            (self.proxy_instance.public_ip_address, "scripts/proxy_script.py", "proxy_script.py"),
            (self.trusted_host_instance.public_ip_address, "scripts/trusted_host_script.py", "trusted_host_script.py"),
            (self.gatekeeper_instance.public_ip_address, "scripts/gatekeeper_script.py", "gatekeeper_script.py"),
        ]

        # Upload scripts to manager, proxy, trusted host, and gatekeeper instances
        for ip_address, local_script_path, remote_script_name in instance_script_mapping:
            try:
                ssh_client = create_ssh_client(ip_address, "ubuntu", self.ssh_key_path)
                scp = SCPClient(ssh_client.get_transport())
                scp.put(local_script_path, remote_script_name)
                scp.put("instances_ips.json", "instances_ips.json")
            except Exception as e:
                print(f"An error occurred while uploading to {ip_address}: {e}")
            finally:
                scp.close()
                ssh_client.close()

        # Upload worker script to each worker instance
        for worker in self.worker_instances:
            try:
                ssh_client = create_ssh_client(worker.public_ip_address, "ubuntu", self.ssh_key_path)
                scp = SCPClient(ssh_client.get_transport())
                scp.put("scripts/worker_script.py", "worker_script.py")
            except Exception as e:
                print(f"Error uploading to worker {worker}: {e}")
            finally:
                scp.close()
                ssh_client.close()

    def execute_flask_apps_on_instances(self) -> None:
    # Execute Flask apps on each instance
        for instance in self.worker_instances + [self.manager_instance]:
            try:
                ssh_client = create_ssh_client(instance.public_ip_address, "ubuntu", self.ssh_key_path)
                script_name = "manager_script.py" if instance == self.manager_instance else "worker_script.py"
                ssh_client.exec_command(f"nohup python3 {script_name} > cluster_output.log 2>&1 &")
                print(f"Started {script_name} on {instance.public_ip_address}")
            except Exception as e:
                print(f"An error occurred while executing on {instance.public_ip_address}: {e}")
            finally:
                ssh_client.close()
        # Execute the script on Proxy instance
        try:
            ssh_client = create_ssh_client(self.proxy_instance.public_ip_address, "ubuntu", self.ssh_key_path)
            ssh_client.exec_command("nohup python3 proxy_script.py > proxy_output.log 2>&1 &")
            print(f"Started proxy_script.py on {self.proxy_instance.public_ip_address}")
        except Exception as e:
            print(f"An error occurred while executing on {self.proxy_instance.public_ip_address}: {e}")
        finally:
            ssh_client.close()

        # Execute the script on Trusted Host instance
        try:
            ssh_client = create_ssh_client(self.trusted_host_instance.public_ip_address, "ubuntu", self.ssh_key_path)
            ssh_client.exec_command("nohup python3 trusted_host_script.py > trusted_host_output.log 2>&1 &")
            print(f"Started trusted_host_script.py on {self.trusted_host_instance.public_ip_address}")
        except Exception as e:
            print(f"An error occurred while executing on {self.trusted_host_instance.public_ip_address}: {e}")
        finally:
            ssh_client.close()

        # Execute the script on Gatekeeper instance
        try:
            ssh_client = create_ssh_client(self.gatekeeper_instance.public_ip_address, "ubuntu", self.ssh_key_path)
            ssh_client.exec_command("nohup python3 gatekeeper_script.py > gatekeeper_output.log 2>&1 &")
            print(f"Started gatekeeper_script.py on {self.gatekeeper_instance.public_ip_address}")
        except Exception as e:
            print(f"An error occurred while executing on {self.gatekeeper_instance.public_ip_address}: {e}")
        finally:
            ssh_client.close()   

    def run_sysbench_benchmark(self):
        """
        Perform MySQL benchmarking using Sysbench on all MySQL instances.
        """
        benchmark_commands = [
            # Prepare the database for Sysbench
            "sudo sysbench /usr/share/sysbench/oltp_read_write.lua --mysql-db=sakila --mysql-user=root --mysql-password='password' prepare",
            
            # Run the Sysbench benchmark
            "sudo sysbench /usr/share/sysbench/oltp_read_write.lua --mysql-db=sakila --mysql-user=root --mysql-password='password' --time=60 --threads=4 run",
            
            # Clean up after benchmarking
            "sudo sysbench /usr/share/sysbench/oltp_read_write.lua --mysql-db=sakila --mysql-user=root --mysql-password='password' cleanup"
        ]

        results = {}

        try:
            # Execute Sysbench commands on all MySQL instances
            for instance in self.worker_instances + [self.manager_instance]:
                ssh_client = create_ssh_client(instance.public_ip_address, "ubuntu", self.ssh_key_path)
                instance_results = []
                print(f"Running Sysbench on instance {instance.public_ip_address}...")

                for command in benchmark_commands:
                    print(f"Executing command: {command} on instance {instance.public_ip_address}")
                    stdin, stdout, stderr = ssh_client.exec_command(command)

                    # Capture output
                    output = stdout.read().decode()
                    error_output = stderr.read().decode()

                    # Log results
                    if "run" in command:
                        instance_results.append(output)  # Save the benchmark result
                        print(f"Benchmark output for {instance.public_ip_address}:\n{output}")
                        # Write the output to a local file
                        filename = f"sysbench_result_{instance.public_ip_address}.txt"
                        with open(filename, 'w') as f:
                            f.write(output)
                        print(f"Sysbench result saved to {filename}")
                    elif error_output:
                        print(f"Error on {instance.public_ip_address}: {error_output}")

                # Store results for the instance
                results[instance.public_ip_address] = instance_results
                ssh_client.close()

        except Exception as e:
            print(f"An error occurred while benchmarking: {e}")

        return results

    
    # Benchmarking of the clusters
    def benchmarks(self):
        """
        Measure response times of MySQL cluster for read and write requests per mode.
        Test all three routing modes: direct_hit, random, and customized.
        """
        import time
        import requests

        gatekeeper_ip = self.gatekeeper_instance.public_ip_address
        gatekeeper_url = f"http://{gatekeeper_ip}:5000"  # Gatekeeper runs on port 5000
        modes = ["direct_hit", "random", "customized"]
        queries = {
            "read": "SELECT * FROM actor LIMIT 10;",
            "write": "INSERT INTO actor (first_name, last_name) VALUES ('Benchmark', 'Test');"
        }

        results = {}

        for mode in modes:
            print(f"\n=== Testing mode: {mode} ===")
            mode_results = {"read_times": [], "write_times": []}
            errors = {"read_errors": [], "write_errors": []}

            for query_type, query in queries.items():
                num_requests = 1000  # Adjust as needed
                times = []
                print(f"  {query_type.capitalize()} Queries:")

                for i in range(num_requests):
                    start_time = time.time()
                    payload = {"query": query, "type": query_type, "mode": mode}
                    try:
                        response = requests.post(gatekeeper_url, json=payload)
                        elapsed_time = time.time() - start_time

                        if response.status_code == 200:
                            times.append(elapsed_time)
                            print(f"    Request {i+1}: Success (Time: {elapsed_time:.4f} seconds)")
                        else:
                            error_message = f"Error {response.status_code}: {response.text}"
                            errors[f"{query_type}_errors"].append(error_message)
                            print(f"    Request {i+1}: Failed - {error_message}")
                    except requests.exceptions.RequestException as e:
                        error_message = f"Request exception: {str(e)}"
                        errors[f"{query_type}_errors"].append(error_message)
                        print(f"    Request {i+1}: Failed - {error_message}")

                average_time = sum(times) / len(times) if times else float('inf')
                mode_results[f"{query_type}_average_time"] = average_time
                print(f"  Average {query_type} time for mode {mode}: {average_time:.4f} seconds")

            results[mode] = mode_results
            results[mode].update(errors)  # Add errors to results

        print("\n=== Benchmark results summary ===")
        for mode, stats in results.items():
            print(f"Mode: {mode}")
            print(f"  Average Read Time: {stats.get('read_average_time', 'N/A'):.4f} seconds")
            print(f"  Average Write Time: {stats.get('write_average_time', 'N/A'):.4f} seconds")
            if stats.get("read_errors"):
                print(f"  Read Errors: {stats['read_errors']}")
            if stats.get("write_errors"):
                print(f"  Write Errors: {stats['write_errors']}")

        return results



# Main 

ec2_manager = EC2Manager()
ec2_manager.create_key_pair()
time.sleep(5)

# Launch instances
instances = ec2_manager.launch_instances()

# Wait for instances to be running
print("Waiting for instances to be running...")
for instance in instances:
    instance.wait_until_running()
    instance.reload()
print("Instances are running.")

time.sleep(10)

ec2_manager.export_instances_public_ips()
ec2_manager._add_inboud_rule_security_group()

time.sleep(5)

print("Running SQL setup on MySQL instances...")
ec2_manager.run_sql_sakila_install()
print("SQL setup complete.")

print("Running Sysbench benchmark on MySQL instances...")
sysbench_results = ec2_manager.run_sysbench_benchmark()
print("Sysbench benchmarks completed. Results are saved in local files.")

ec2_manager.configure_instance(ec2_manager.proxy_instance)
ec2_manager.configure_instance(ec2_manager.gatekeeper_instance)
ec2_manager.configure_instance(ec2_manager.trusted_host_instance)

# Upload Flask apps to instances
print("Uploading Flask apps to instances...")
ec2_manager.upload_flask_apps_to_instances()

# Execute Flask apps on instances
print("Executing Flask apps on instances...")
ec2_manager.execute_flask_apps_on_instances()

print("Waiting for instances to be ready...")
time.sleep(20)

# Send requests to the Gatekeeper for benchmarking
print("Sending requests to the Gatekeeper...")
benchmark_results = ec2_manager.benchmarks()
print(f"Benchmark results: {benchmark_results}")

# Cleaning up
input("Press Enter to terminate and cleanup all resources...")
ec2_manager.cleanup()
print("Cleanup complete.")