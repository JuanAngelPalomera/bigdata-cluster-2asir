Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"
  config.vm.boot_timeout = 600

  nodes = [
    { name: "master",  ip: "192.168.56.10", memory: 4096, cpus: 2 },
    { name: "worker1", ip: "192.168.56.11", memory: 2048, cpus: 2 },
    { name: "worker2", ip: "192.168.56.12", memory: 2048, cpus: 2 }
  ]

  nodes.each do |node|
    config.vm.define node[:name] do |n|
      n.vm.hostname = node[:name]
      n.vm.network "private_network", ip: node[:ip]

      n.vm.provider "virtualbox" do |vb|
        vb.name   = "bigdata-#{node[:name]}"
        vb.memory = node[:memory]
        vb.cpus   = node[:cpus]
        vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
        vb.customize ["modifyvm", :id, "--uartmode1", "disconnected"]
      end
      n.vm.provision "shell", inline: <<-SHELL
        mkdir -p /etc/systemd/resolved.conf.d
        printf '[Resolve]\nDNS=8.8.8.8 1.1.1.1\nFallbackDNS=9.9.9.9\n' > /etc/systemd/resolved.conf.d/dns.conf
        systemctl restart systemd-resolved
      SHELL
      
      n.vm.provision "ansible_local" do |ansible|
        ansible.playbook         = "ansible/site.yml"
        ansible.inventory_path   = "ansible/inventory.ini"
        ansible.limit            = node[:name]
        ansible.galaxy_role_file = "ansible/requirements.yml"
        ansible.galaxy_command   = "ansible-galaxy collection install -r %{role_file}"
      end
    end
  end
end