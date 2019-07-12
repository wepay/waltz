# prepare keystore and truststore file
sudo cp /vagrant/keystore.jks /etc/waltz-storage
sudo cp /vagrant/truststore.jks /etc/waltz-storage

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
