#!/bin/bash
mv /home/ec2-user/featurebase_linux_amd64 /usr/local/bin/featurebase
mv /home/ec2-user/featurebase.conf /etc/
mv /home/ec2-user/featurebase.service /etc/systemd/system/
adduser molecula
sudo mkdir /var/log/molecula
sudo chown molecula /var/log/molecula
sudo mkdir -p /opt/molecula/featurebase
sudo chown molecula /opt/molecula/featurebase
systemctl daemon-reload
sudo systemctl start featurebase
sudo systemctl enable featurebase
sudo systemctl status featurebase
curl localhost:10101
