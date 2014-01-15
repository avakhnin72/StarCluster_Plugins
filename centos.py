from starcluster.clustersetup import ClusterSetup
from starcluster.logger import log

global repo_to_install
global pkg_to_install
global svc_to_disable
global svcs

class WgetPackages(ClusterSetup):
	def __init__(self,pkg_to_wget):
		self.pkg_to_wget = pkg_to_wget
		log.debug('pkg_to_wget = %s' % pkg_to_wget)
	def run(self, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Wgetting %s on %s" % (self.pkg_to_wget, node.alias))
			node.ssh.execute('wget %s' % self.pkg_to_wget)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Wgetting %s on %s" % (self.pkg_to_wget, node.alias))
			node.ssh.execute('wget %s' % self.pkg_to_wget)

class RpmInstaller(ClusterSetup):
	def __init__(self,rpm_to_install):
		self.rpm_to_install = rpm_to_install
		log.debug('rpm_to_install = %s' % rpm_to_install)
	def run(self, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.rpm_to_install, node.alias))
			node.ssh.execute('yum -y --nogpgcheck localinstall %s' %self.rpm_to_install)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.rpm_to_install, node.alias))
			node.ssh.execute('yum -y --nogpgcheck localinstall %s' %self.rpm_to_install)

class RepoConfigurator(ClusterSetup):
	def __init__(self,repo_to_install):
		self.repo_to_install  = repo_to_install
		log.debug('repo_to_install = %s' % repo_to_install)
	def run(self, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.repo_to_install, node.alias))
			node.ssh.execute('rpm --import %s' % self.repo_to_install)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.repo_to_install, node.alias))
			node.ssh.execute('rpm --import %s' % self.repo_to_install)

class PackageInstaller(ClusterSetup):
	def __init__(self,pkg_to_install):
		self.pkg_to_install  = pkg_to_install
		log.debug('pkg_to_install = %s' % pkg_to_install)
	def run(self, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.pkg_to_install, node.alias))
			node.ssh.execute('yum -y install %s' % self.pkg_to_install)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.pkg_to_install, node.alias))
			node.ssh.execute('yum -y install %s' % self.pkg_to_install)

class ExecBinary(ClusterSetup):
	def __init__(self,bin_to_exec):
		self.bin_to_exec = bin_to_exec
		log.debug('bin_to_exec = %s' % bin_to_exec)
	def run(self, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.bin_to_exec, node.alias))
			node.ssh.execute('bash -x %s' % self.bin_to_exec)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Installing %s on %s" % (self.bin_to_exec, node.alias))
			node.ssh.execute('bash -x %s' % self.bin_to_exec)
class DisableServices(ClusterSetup):
	def __init__(self,svc_to_disable):
		self.svc_to_disable = svc_to_disable
		log.debug('svc_to_disable = %s' % svc_to_disable)
	def run(self, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Disabling %s on %s" % (self.svc_to_disable, node.alias))
			node.ssh.execute('/sbin/chkconfig --level 235 %s off' % self.svc_to_disable)
	def on_add_node(self, node, nodes, master, user, user_shell, volumes):
		for node in nodes:
			log.info("Disabling %s on %s" % (self.svc_to_disable, node.alias))
			node.ssh.execute('/sbin/chkconfig --level 235 %s off' % self.svc_to_disable)
