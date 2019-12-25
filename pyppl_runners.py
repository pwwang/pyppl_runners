
from os import utime, getcwd
import sys
import re
import cmdy
from diot import OrderedDiot
from pyppl.runner import hookimpl, PyPPLRunnerLocal
from pyppl.utils import filesig
from pyppl.logger import logger
from pyppl._proc import OUT_FILETYPE, OUT_DIRTYPE

__version__ = "0.0.2"

class PyPPLRunnerDry(PyPPLRunnerLocal):
	"""@API
	The dry runner
	"""
	__version__ = __version__

	@hookimpl
	def script_parts(self, job, base):
		# we should not cache from previous results
		job.proc.cache = False

		base.pre += '\n'
		base.pre += '# Dry-run script to create empty output files and directories.\n'
		base.pre += '\n'

		for vtype, value in job.output.values():
			if vtype in OUT_FILETYPE:
				base.pre += "touch %s\n" % cmdy._shquote(str(job.dir / 'output' / value))
			elif vtype in OUT_DIRTYPE:
				base.pre += "mkdir -p %s\n" % cmdy._shquote(str(job.dir / 'output' / value))
		# don't run the real script
		base.command = ''
		base.saveoe  = False

		# we should also prevent further run caching from this results
		script_mtime = filesig(job.dir / 'job.script')
		utime(job.dir / 'job.script', (script_mtime[1] + 1000, ) * 2)
		return base

class PyPPLRunnerSsh(PyPPLRunnerLocal):
	"""@API
	The ssh runner
	@static variables:
		LIVE_SERVERS (list): The live servers
	"""
	__version__ = __version__

	LIVE_SERVERS = OrderedDiot()
	SSH          = None

	@staticmethod
	def is_server_alive(server, key = None, timeout = 3, ssh = 'ssh'):
		"""@API
		Check if an ssh server is alive
		@params:
			server (str): The server to check
			key (str): The keyfile to login the server
			timeout (int|float): The timeout to check whether the server is alive.
		@returns:
			(bool): `True` if alive else `False`
		"""
		params = {'': server, '_timeout': timeout, '_': 'true', '_dupkey': True, '_raise': False}
		if key:
			params['i'] = key
		params['o']    = ['BatchMode=yes', 'ConnectionAttempts=1']
		params['_exe'] = ssh
		try:
			cmd = cmdy.ssh(**params)
			return cmd.rc == 0
		except cmdy.CmdyTimeoutException:
			return False

	@hookimpl
	def runner_init(self, proc):
		ssh         = proc.runner.get('ssh_ssh', 'ssh')
		servers     = proc.runner.get('ssh_servers', [])
		keys        = proc.runner.get('ssh_keys', [])
		if not servers:
			raise ValueError('No server specified for ssh runner.')

		if not PyPPLRunnerSsh.LIVE_SERVERS:
			logger.debug('Checking status of servers ...', proc = proc.id)
			for i, server in enumerate(servers):
				key = False if not keys else keys[i]
				if PyPPLRunnerSsh.is_server_alive(server, key, ssh = ssh):
					PyPPLRunnerSsh.LIVE_SERVERS[server] = key

		if not PyPPLRunnerSsh.LIVE_SERVERS:
			raise ValueError('No server is alive.')

		PyPPLRunnerSsh.SSH = cmdy.ssh.bake(
			_dupkey = True, _raise = False, _exe = ssh)

	@hookimpl
	def script_parts(self, job, base):
		server = list(PyPPLRunnerSsh.LIVE_SERVERS.keys())[
			job.index % len(PyPPLRunnerSsh.LIVE_SERVERS)]
		key = PyPPLRunnerSsh.LIVE_SERVERS[server]
		job.ssh = PyPPLRunnerSsh.SSH.bake(t = server, i = key)
		base.header  = '#\n# Running job on server: %s\n#' % server
		base.pre    += '\ncd %s' % cmdy._shquote(getcwd())
		return base

	@hookimpl
	def submit(self, job):
		"""
		Submit the job
		@returns:
			The `utils.cmd.Cmd` instance if succeed
			else a `Diot` object with stderr as the exception and rc as 1
		"""
		cmd = job.ssh(_ = cmdy.ls(job.dir.joinpath('job.script'), _hold = True, _raise = False).cmd)
		if cmd.rc != 0:
			dbox        = Diot()
			dbox.rc     = cmd.rc
			dbox.cmd    = cmd.cmd
			dbox.pid    = -1
			dbox.stderr = cmd.stderr
			dbox.stderr += '\nProbably the server ({})'.format(job.ssh.keywords['t'])
			dbox.stderr += ' is not using the same file system as the local machine.\n'
			return dbox

		cmd = job.ssh(_bg = True, _ = job.script)
		cmd.rc = 0
		job.pid = cmd.pid
		return cmd

	@hookimpl
	def kill(self, job):
		"""
		Kill the job
		"""
		cmd = cmdy.python(
			c      = 'from pyppl.utils import killtree; killtree(%s, killme = True)' % job.pid,
			_exe   = sys.executable,
			_raise = False,
			_hold  = True).cmd
		job.ssh(_ = cmd)

	@hookimpl
	def isrunning(self, job):
		"""
		Tell if the job is alive
		@returns:
			`True` if it is else `False`
		"""
		try:
			if int(job.pid) < 0:
				return False
		except (TypeError, ValueError):
			return False

		cmd = cmdy.python(
			c = 'from psutil import pid_exists; ' + \
				'assert {pid} > 0 and pid_exists({pid})'.format(pid = job.pid),
			_raise = False,
			_exe   = sys.executable,
			_hold  = True).cmd
		return job.ssh(_ = cmd).rc == 0

class PyPPLRunnerSge:
	__version__ = __version__
	POLL_INTERVAL = 5
	CMD_QSUB = CMD_QSTAT = CMD_QDEL = None

	@hookimpl
	def runner_init(self, proc):
		PyPPLRunnerSge.CMD_QSUB = cmdy.qsub.bake(
			_exe = proc.runner.get('sge_qsub', 'qsub'), _raise = False)
		PyPPLRunnerSge.CMD_QSTAT = cmdy.qsub.bake(
			_exe = proc.runner.get('sge_qstat', 'qstat'), _raise = False)
		PyPPLRunnerSge.CMD_QDEL = cmdy.qsub.bake(
			_exe = proc.runner.get('sge_qdel', 'qdel'), _raise = False)

	@hookimpl
	def script_parts(self, job, base):
		sge_n = job.proc.runner.get('sge_N', '%s.%s.%s.%s' % (
			job.proc.id,
			job.proc.tag.replace('@', '_'), # fix @ not allowed in job names
			job.proc.suffix,
			job.index + 1))
		base.header += '#$ -N %s\n' % job.proc.template(
			sge_n, **job.proc.envs).render(job.data)
		base.header += '#$ -cwd\n'
		base.header += '#$ -o %s\n' % (job.dir / 'job.stdout')
		base.header += '#$ -e %s\n' % (job.dir / 'job.stderr')

		for key in sorted(job.proc.runner):
			if not key.startswith ('sge_') or key in (
				'sge_N', 'sge_qsub', 'sge_qstat', 'sge_qdel'):
				continue
			if key in ('sge_o', 'sge_e', 'sge_cwd'):
				raise ValueError('-o, -e and -cwd are not allowed to be configured.')
			val = job.proc.runner[key]
			key = key[4:]
			# {'notify': True} ==> -notify
			src = key if val is True else key + ' ' + str(val)
			base.header += '#$ -%s\n' % src

		base.saveoe = False
		return base

	@hookimpl
	def submit(self, job):
		"""
		Submit the job
		@returns:
			The `utils.cmd.Cmd` instance if succeed
			else a `Diot` object with stderr as the exception and rc as 1
		"""
		cmd = PyPPLRunnerSge.CMD_QSUB(job.script[0])
		if cmd.rc == 0:
			# Your job 6556149 ("pSort.notag.3omQ6NdZ.0") has been submitted
			match = re.search(r'\s(\d+)\s', cmd.stdout.strip())
			if not match:
				cmd.rc = 1
			else:
				job.pid = match.group(1)
		return cmd

	@hookimpl
	def kill(self, job):
		"""
		Kill the job
		"""
		PyPPLRunnerSge.CMD_QDEL(force = job.pid)

	@hookimpl
	def isrunning(self, job):
		"""
		Tell if the job is alive
		@returns:
			`True` if it is else `False`
		"""
		if not job.pid:
			return False
		return PyPPLRunnerSge.CMD_QSTAT(j = job.pid).rc == 0

class PyPPLRunnerSlurm:
	__version__ = __version__
	POLL_INTERVAL = 5
	CMD_SBATCH = CMD_SRUN = CMD_SCANCEL = CMD_SQUEUE = None

	@hookimpl
	def runner_init(self, proc):
		PyPPLRunnerSlurm.CMD_SBATCH = cmdy.qsub.bake(
			_exe = proc.runner.get('slurm_sbatch', 'sbatch'), _raise = False)
		PyPPLRunnerSlurm.CMD_SRUN = cmdy.qsub.bake(
			_exe = proc.runner.get('slurm_srun', 'srun'), _raise = False)
		PyPPLRunnerSlurm.CMD_SCANCEL = cmdy.qsub.bake(
			_exe = proc.runner.get('slurm_scancel', 'scancel'), _raise = False)
		PyPPLRunnerSlurm.CMD_SQUEUE = cmdy.qsub.bake(
			_exe = proc.runner.get('slurm_squeue', 'squeue'), _raise = False)

	@hookimpl
	def script_parts(self, job, base):
		slurm_j = job.proc.runner.get('slurm_J', '%s.%s.%s.%s' % (
			job.proc.id,
			job.proc.tag.replace('@', '_'), # fix @ not allowed in job names
			job.proc.suffix,
			job.index + 1))
		base.header += '#SBATCH -J %s\n' % job.proc.template(
			slurm_j, **job.proc.envs).render(job.data)
		base.header += '#SBATCH -o %s\n' % (job.dir / 'job.stdout')
		base.header += '#SBATCH -o %s\n' % (job.dir / 'job.stderr')

		for key in sorted(job.proc.runner):
			if not key.startswith ('slurm_') or key in (
				'slurm_J', 'slurm_srun_opts', 'slurm_sbatch',
				'slurm_srun', 'slurm_scancel', 'slurm_squeue'):
				continue
			if key in ('slurm_o', 'slurm_e'):
				raise ValueError('-o and -e are not allowed to be configured.')
			val = job.proc.runner[key]
			key = key[6:]
			# {'notify': True} ==> -notify
			if len(key) == 1:
				src = '-' + key if val is True else '-' + key + ' ' + str(val)
			else:
				src = '--' + key if val is True else '--' + key + '=' + str(val)
			base.header += '#SBATCH %s\n' % src

		base.saveoe = False
		srunopts = job.proc.runner.get('slurm_srun_opts', '').split()
		srunopts.extend(base.command)
		base.command = PyPPLRunnerSlurm.CMD_SRUN(*srunopts, _hold = True).cmd
		return base

	@hookimpl
	def submit(self, job):
		"""
		Submit the job
		@returns:
			The `utils.cmd.Cmd` instance if succeed
			else a `Diot` object with stderr as the exception and rc as 1
		"""
		cmd = PyPPLRunnerSlurm.CMD_SBATCH(job.script[0])
		if cmd.rc == 0:
			# Your job 6556149 ("pSort.notag.3omQ6NdZ.0") has been submitted
			match = re.search(r'\s(\d+)$', cmd.stdout.strip())
			if not match:
				cmd.rc = 1
			else:
				job.pid = match.group(1)
		return cmd

	@hookimpl
	def kill(self, job):
		"""
		Kill the job
		"""
		PyPPLRunnerSlurm.CMD_SCANCEL(job.pid)

	@hookimpl
	def isrunning(self, job):
		"""
		Tell if the job is alive
		@returns:
			`True` if it is else `False`
		"""
		if not job.pid:
			return False
		return PyPPLRunnerSlurm.CMD_SQUEUE(j = job.pid).rc == 0

dry = PyPPLRunnerDry()
ssh = PyPPLRunnerSsh()
sge = PyPPLRunnerSge()
slurm = PyPPLRunnerSlurm()
