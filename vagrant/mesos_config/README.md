# Configuring the mesos package provided by mesosphere

The mesosphere mesos package for debian is configured by creating files whose names match command
line argument and environment variables.

When setting up our environment, the files under directories `etc_mesos-master` and
`etc_mesos-slave` are copied to `/etc/mesos-master` and `/etc/mesos-slave`, respectively.

For more details, and the script that reads these files, see
[here](https://github.com/mesosphere/mesos-deb-packaging/blob/fd3c3866a847d07a8beb0cf8811f173406f910df/mesos-init-wrapper#L12-L48).
