Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"

  nodes = [
    { name: "master",  ip: "192.168.56.10", memory: 2048, cpus: 2 },
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
      end
    end
  end
end