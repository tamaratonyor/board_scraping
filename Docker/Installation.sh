#!/usr/bin/bash
sudo dpkg --configure -a

sudo apt-get remove docker docker-engine docker.io
sudo apt install docker.io
sudo systemctl start docker
sudo systemctl enable docker
apt --fix-broken install 

usermod -a -G docker <your user name>
sudo systemctl restart docker

cd ~

git clone https://github.com/entrancestone/docker-airflow-webscrapping.git

cd docker-airflow-webscrapping

sudo docker-compose up
