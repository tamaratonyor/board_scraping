#!/usr/bin/bash
sudo apt-get update
sudo dpkg --configure -a

sudo apt-get remove docker docker-engine docker.io
sudo apt install docker.io
sudo systemctl start docker
sudo systemctl enable docker
apt --fix-broken install 
