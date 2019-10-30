require 'inifile'
require 'json'

# read config file
file = IniFile.load('waltz-test/src/main/python/waltz_ducktape/config.ini')
cfg = file['Vagrant Google']

Vagrant.configure("2") do |config|
  # Eanble hostmanager so nodes can talk to eachother by DNS
  config.hostmanager.enabled = true
  config.hostmanager.manage_host = true
  config.hostmanager.include_offline = false

  config.vm.box = "google/gce"

  # Provision waltz-storage nodes
  (0..3).each do |i|
    config.vm.define "storage-#{i}" do |node|
      node.vm.provider :google do |google, override|
        # google project & service account
        google.google_project_id = cfg['GoogleProjectId']
        google.google_client_email = cfg['GoogleClientEmail']
        google.google_json_key_location = cfg['GoogleJsonKeyLocation']

        # gce image config
        google.image = cfg['WaltzStorageImage']
        google.image_project_id = cfg['GoogleProjectId']
        google.disk_size = Integer(cfg['WaltzStorageBootDiskSize'])

        # additional image config
        google.additional_disks = JSON.parse(cfg['WaltzStorageAdditionalDisks'], {:symbolize_names => true})

        google.external_ip = false
        google.use_private_ip = true

        google.name = cfg['WaltzStorageInstanceName'] + "-#{i}"
        google.machine_type = cfg['WaltzStorageInstanceMachineType']
        google.zone = JSON.parse(cfg['WaltzStorageInstanceZones'])[i/3]

        # the instance needs to be on the poc network
        google.network = cfg['GoogleNetwork']
        google.subnetwork = cfg['GoogleSubnetwork']

        override.ssh.username = cfg['WaltzSshUsername']
        override.ssh.private_key_path = cfg['WaltzSshPrivateKeyPath']
      end

      # Mount the synced folders
      node.vm.synced_folder "vagrant/sync/storage", "/vagrant"

      # Provision for Waltz storage
      node.vm.provision "shell", path: "script/storage-provision.sh"
    end
  end

  # Provision waltz-server nodes
  (0..1).each do |i|
    config.vm.define "server-#{i}" do |node|
      node.vm.provider :google do |google, override|
        # google project & service account
        google.google_project_id = cfg['GoogleProjectId']
        google.google_client_email = cfg['GoogleClientEmail']
        google.google_json_key_location = cfg['GoogleJsonKeyLocation']

        # gce image config
        google.image = cfg['WaltzServerImage']
        google.image_project_id = cfg['GoogleProjectId']
        google.disk_size = Integer(cfg['WaltzServerBootDiskSize'])

        google.external_ip = false
        google.use_private_ip = true

        google.name = cfg['WaltzServerInstanceName'] + "-#{i}"
        google.machine_type = cfg['WaltzServerInstanceMachineType']
        google.zone = cfg['WaltzServerInstanceZone']

        google.network = cfg['GoogleNetwork']
        google.subnetwork = cfg['GoogleSubnetwork']

        override.ssh.username = cfg['WaltzSshUsername']
        override.ssh.private_key_path = cfg['WaltzSshPrivateKeyPath']
      end

      # Mount the synced folders
      node.vm.synced_folder "vagrant/sync/server", "/vagrant"

      # Provision for Waltz server
      node.vm.provision "shell", path: "script/server-provision.sh"
    end
  end

  # Provision waltz-client nodes
  (0..0).each do |i|
    config.vm.define "client-#{i}" do |node|
      node.vm.provider :google do |google, override|
        # google project & service account
        google.google_project_id = cfg['GoogleProjectId']
        google.google_client_email = cfg['GoogleClientEmail']
        google.google_json_key_location = cfg['GoogleJsonKeyLocation']

        # gce image config
        google.image = cfg['WaltzClientImage']
        google.image_project_id = cfg['GoogleProjectId']
        google.disk_size = Integer(cfg['WaltzClientBootDiskSize'])

        google.external_ip = false
        google.use_private_ip = true

        google.name = cfg['WaltzClientInstanceName'] + "-#{i}"
        google.machine_type = cfg['WaltzClientInstanceMachineType']
        google.zone = cfg['WaltzClientInstanceZone']

        google.network = cfg['GoogleNetwork']
        google.subnetwork = cfg['GoogleSubnetwork']

        override.ssh.username = cfg['WaltzSshUsername']
        override.ssh.private_key_path = cfg['WaltzSshPrivateKeyPath']
      end

      # Mount the synced folders
      node.vm.synced_folder "vagrant/sync/client", "/vagrant"

      # Provision for Waltz client
      node.vm.provision "shell", path: "script/client-provision.sh"
    end
  end

end
