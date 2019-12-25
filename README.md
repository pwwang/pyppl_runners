# pyppl_runners

A set of common runners for PyPPL, including

- `dry`: Dry run the pipeline without running the job sccripts
- `ssh`: Use CPUs shared via ssh (servers have to share the same file system
- `sge`: Use the Sun Grid Engine
- `slurm`: Use the Slurm Engine

## Configurations

Configurations for runners are set in `pXXX.runner` or `runner` item in configuration

*dry*

- None

*ssh*

- `ssh_servers (list)`: IP or name of SSH servers
- `ssh_keys (list)`: Corresponding SSH keys for the servers
- `ssh_ssh (str)`: Path to `ssh` command

*sge*

- `sge_qsub (str)`: Path to `qsub` command
- `sge_qstat (str)`: Path to `qstat` command
- `sge_qdel (str)`: Path to `qdel` command
- `sge_N (str)`: A template of job name using `job.data` to render
- `sge_<X>`: SGE option `<X>`

Note that `sge_cwd`, `sge_o`, `sge_e` are not allowed to be configured

*slurm*

- `slurm_sbatch`: Path to `sbatch` command
- `slurm_srun`: Path to `srun` command
- `slurm_squeue`: Path to `squeue` command
- `slurm_scancel`: Path to `scancel` command
- `slurm_srun_opts`: Options for `srun`
- `slurm_J (str)`: A template of job name using `job.data` to render
- `slurm_<X>`: Slurm option `<X>`

Note that `slurm_o`, `slurm_e` are not allowed to be configured
