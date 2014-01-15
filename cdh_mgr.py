import posixpath

from starcluster import threadpool
from starcluster import clustersetup
from starcluster.logger import log

class ClouderaMgr(clustersetup.ClusterSetup):

	def __init__(self,cdh_mgr_agentdir='/etc/cloudera-scm-agent'):
		self.cdh_mgr_agentdir = cdh_mgr_agentdir
		self.cdh_mgr_agent_conf = '/etc/cloudera-scm-agent/config.ini'
		self.cdh_mgr_repo_conf = '/etc/yum.repos.d/cloudera-cdh4.repo'
		self.cdh_mgr_repo_url = 'http://archive.cloudera.com/cm4/redhat/6/x86_64/cm/cloudera-manager.repo'
		self._pool = None

	@property
	def pool(self):
		if self._pool is None:
			self._pool = threadpool.get_thread_pool(20, disable_threads=False)
		return self._pool

	def _install_cdhmgr_repo(self,node):
		node.ssh.execute('wget %s' % self.cdh_mgr_repo_url)
		node.ssh.execute('cat /root/cloudera-manager.repo >> %s' % self.cdh_mgr_repo_conf)

	def _install_cdhmgr_agent(self,node):
		node.ssh.execute('yum install -y cloudera-manager-agent')
		node.ssh.execute('yum install -y cloudera-manager-daemons')

	def _install_cdhmgr(self,master):
		master.ssh.execute('/sbin/service cloudera-scn-agent stop')
		master.ssh.execute('/sbin/chkconfig cloudera-scn-agent off')
		master.ssh.execute('/sbin/chkconfig hue off')
		master.ssh.execute('/sbin/chkconfig oozie off')
		master.ssh.execute('/sbin/chkconfig hadoop-httpfs off')
		master.ssh.execute('yum install -y cloudera-manager-server')
		master.ssh.execute('yum install -y cloudera-manager-server-db')
		master.ssh.execute('/sbin/service cloudera-scm-server-db start')
		master.ssh.execute('/sbin/service cloudera-scm-server start')

        def _setup_hadoop_user(self,node,user):
		node.ssh.execute('gpasswd -a %s hadoop' %user)
	
	def _install_agent_conf(self, node):
		node.ssh.execute('/bin/sed -e"s/server_host=localhost/server_host=master/g" self.cdh_mgr_agent_conf > /tmp/config.ini; mv /tmp/config.ini self.cdh_mgr_agent_conf')

	def _open_ports(self,master):
		ports = [7180,50070,50030]
		ec2 = master.ec2
		for group in master.cluster_groups:
			for port in ports:
				has_perm = ec2.has_permission(group, 'tcp', port, port, '0.0.0.0/0')
				if not has_perm:
					ec2.conn.authorize_security_group(group_id=group.id,
									ip_protocol='tcp',
									from_port=port,
									to_port=port,
									cidr_ip='0.0.0.0/0')


	def run(self,nodes, master, user, user_shell, volumes):
		for node in nodes:
		     self._install_cdhmgr_repo(node)
		     self._install_cdhmgr_agent(node)
		     self._install_agent_conf(node)
		self._install_cdhmgr(master)
		self._open_ports(master)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
                for node in nodes:
		     self._install_cdhmgr_repo(node)
		     self._install_cdhmgr_agent(node)
		     self._install_agent_conf(node)
