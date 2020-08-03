# prepare keystore and truststore file
sudo mkdir /etc/waltz
sudo chmod a+w /etc/waltz
sudo cp /vagrant/"$(hostname)".jks /etc/waltz/keystore.jks
sudo cp /vagrant/truststore.jks /etc/waltz

# mount sdb
sudo mkdir -p /waltzdata
sudo mount -o discard,defaults,noatime /dev/sdb /waltzdata
sudo chmod a+w /waltzdata

# check mount status
if mountpoint -q /waltzdata
then
  echo "Mount succeeded!"
else
  echo "Mount failed: `mountpoint /waltzdata`" >&2 && exit 1
fi
