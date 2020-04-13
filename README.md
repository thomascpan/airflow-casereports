# airflow-casereports

docker-compose up -d --build
docker-compose down
Go to http://localhost:8080/


## Deployment
```
# Update YUM
sudo yum update

# Install xclip
sudo yum install xclip

# Install Docker and Docker-Compose
sudo yum install docker
sudo curl -L https://github.com/docker/compose/releases/download/1.21.0/docker-compose-`uname -s`-`uname -m` | sudo tee /usr/local/bin/docker-compose > /dev/null
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# Install Git
sudo yum install git -y

# Start Docker Service
sudo service docker start

# Start Airflow
docker-compose up -d
```