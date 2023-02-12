# Week 1: Basic and setup
Prepare all the tools we need like docker, postgre, pgadmin, and terraform to meet the prerequisite tools that we will use in this zoomcamp. in this zoomcamp we are using [ny_taxi data](https://github.com/DataTalksClub/nyc-tlc-data) as the main data source


<details open>
<summary>Run services in google cloud VM</summary>

**Links**:

https://cloud.google.com/compute/docs/connect/create-ssh-keys
https://www.anaconda.com/products/distribution
https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md
https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64
https://www.digitalocean.com/community/tutorials/how-to-use-sftp-to-securely-transfer-files-with-a-remote-server
https://releases.hashicorp.com/terraform/1.3.7/terraform_1.3.7_linux_amd64.zip


### **SSH**
**generate ssh key**
```
ssh-keygen -t rsa -f ~/.ssh/{KEY_FILENAME} -C {USERNAME} -b 2048
```
**create .ssh**
```
cd ~
mkdir .ssh
cd .ssh/
```

**connect into vm with external ip address**
```
ssh -i ~/.ssh/{PRIVATE_KEY} {USERNAME}}@{EXTERNAL_IPADDRESS}
ssh -i ~/.ssh/gcp ersa@34.101.110.221
```
> -i: stands for identifier

**Create ssh config file**
```
cd ~/.ssh/
touch config
```

```nano config``` or ```code config``` to insert the ssh configuration

```
HOST de-zoomcamp
    HostName {EXTERNAL_IPADDRESS}
    User ersa
    IdentityFile c:/Users/X1/.ssh/gcp
```

### **Setup Docker**
first thing you must to do is update the package resources
```
sudo apt update
sudo apt install docker.io
```

**setup docker without sudo**

1. create group if doesnt exist
```
$ sudo groupadd docker
```

2. Add username into group
```
$ sudo gpasswd -a $USER docker
```

3. relogin the ssh connection to re-evaluated group membership
> [CTRL-D] and connect again
4. restart docker daemon 

```
$ sudo service docker restart
```

### **Setup docker-compose**
> create executable file can be executed anywhere, in case docker-compose
1. download docker-compose 
```
mkdir bin/

wget https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64 -O docker-compose

chmod +x docker-compose
```
2. export path
```
nano .bashrc 
export PATH="${HOME}/bin:${PATH}"
```

3. load bashrc config 
```
source .bashrc
```

### **Install pgcli**
1. ```pip install pgcli```

or 

1. ```conda install -c conda-forge pgcli```
2. ```pip install -U mycli```

### **Put file in vm using sftp**
1. change directory to selected directory

2. upload file using sftp
```
sftp de-zoomcamp
cd de-zoomcamp/week_1_basics_n_setup/2_docker_sql
put yellow_tripdata_2021-01.csv
```

### **Terraform**
**Install terraform**
1. sudo apt install unzip
2. unzip terraform_1.3.7_linux_amd64.zip


**Put credentials**
1. change directory to selected directory
2. put file into vm
```
sftp de-zoomcamp
mkdir .gc
cd .gc/
put credentials.json
```
**Authenticate our cli** 

1. export variable 
```
export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/credentials.json
```
2. do auth for the cli
```
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
```

***VM credentials using file*** *(alt: if you want to auth the terraform with your credentials and put credentials file in the same folder with tf files)*

after create credentials, put the credentials in the terraform folder rename into "credentials.json" (it's up to you btw)

add this in provider block
```
credentials = file("credentials.json")
```
your provider block looks like this: 
```
provider "google" {
  project = var.project
  region = var.region
  credentials = file("credentials.json")
}
```
</details>
