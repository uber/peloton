# Peloton development environment

This directory contains [Packer](https://packer.io) scripts
to build and distribute the base development environment for Peloton.

The goal of this environment is to pre-fetch dependencies and artifacts
needed for the integration test environment of Peloton so that `vagrant up` is
cheap after the box has been fetched for the first time.

## Updating the box

1. Download [packer](https://www.packer.io/downloads.html)

2. Modify build scripts to make the changes you want
   (e.g. install packages via `apt`)

3. Fetch the latest version of our base box

        $ vagrant box update --box ubuntu/trusty64

    The box will be stored in version-specific directories under
    `~/.vagrant.d/boxes/ubuntu-VAGRANTSLASH-trusty64/`.  Find the path to the `.ovf` file for the
    latest version of the box.  In the following step, this path will be referred to as
    `$UBUNTU_OVF`.

4. Build the new box
    Using the path from the previous step, run the following command to start the build.

        $ packer build -var "base_box_ovf=$UBUNTU_OVF" peloton.json

    This takes a while, approximately 20 minutes.  When finished, your working directory will
    contain a file named `packer_virtualbox-ovf_virtualbox.box`.

5. Verify your box locally

        $ vagrant box add --name peloton-dev-testing \
          packer_virtualbox-ovf_virtualbox.box

    This will make a vagrant box named `peloton-dev-testing` locally available to vagrant
    (i.e. not on Vagrant Cloud).  We use a different name here to avoid confusion that could
    arise from using an unreleased base box.

    Edit the [`Vagrantfile`](../../Vagrantfile), changing the line

        config.vm.box = "mincai/peloton-dev"

    to

        config.vm.box = "peloton-dev-testing"

    and comment out vm version

        # config.vm.box_version = "0.0.X"

    At this point, you can use the box as normal to run integration tests.

6. Upload the box to Vagrant Cloud
    Our boxes are stored [here](https://atlas.hashicorp.com/mincai/boxes/peloton-dev-trusty).
