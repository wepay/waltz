# prepare keystore and truststore file
sudo mkdir /etc/waltz
sudo chmod a+w /etc/waltz
sudo cp /vagrant/"$(hostname)".jks /etc/waltz/keystore.jks
sudo cp /vagrant/truststore.jks /etc/waltz
