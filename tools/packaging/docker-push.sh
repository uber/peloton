#!/usr/bin/env bash
# takes an list of images (built locally) like uber/peloton:foo123, and tags and
# pushes to some registries. By default, it will push the image to both ATG and
# SJC1 registries with DC=all. You can override with DC=atg or DC=sjc1

DC="${DC:-all}"
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

if [[ $# -eq 0 ]] ; then
  echo "No image passed; we dont know what image to retag and push" >&2
  exit 1
fi

if [[ $DC == all ]] ; then
  DC="sjc1 atg"
fi

for dc in $DC ; do
  for image in "${@}" ; do
    echo "Pushing $image to $dc..."
    new_image="vendor/peloton"
    if [[ $dc == atg ]] ; then
      registry="docker-registry.pit-irn-1.uberatc.net"
      #TODO this is interactive; use login flags to automate?
      $docker_cmd login "$registry"
    else
      registry="docker-registry02-sjc1:5055"
    fi
    # pull version from the image, assume latest if not present
    ver="${image##*:}"
    if [[ "$ver" != "$image" ]] ; then
      version="${ver:-latest}"
    else
      version="latest"
    fi
    push_target="${registry}/${new_image}:${version}"
    $docker_cmd tag "${image}" "${push_target}"
    $docker_cmd push "${push_target}"
    echo "The image can now be pulled from docker registry at ${push_target}"
  done
done
