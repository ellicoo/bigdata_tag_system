#!/bin/bash

set -e

function usage_hint() {
  echo "Usage:"
  echo "generate_env_pyspark.sh [-p] [-r] [-t] [-c] [-h]"
  echo "Description:"
  echo "-p ARG, the version of python, currently supports python 2.7, 3.5, 3.6 and 3.7 versions."
  echo "-r ARG, the local path of your python requirements."
  echo "-t ARG, the output directory of the gz compressed package."
  echo "-c, clean mode, we will only package python according to your requirements, without other pre-provided dependencies."
  echo "-h, display help of this script."
}

function check_parameters() {
  case $pythonVersion in
  2.7) ;;
  3.5) ;;
  3.6) ;;
  3.7) ;;
  *)
    echo "Not support python version [$pythonVersion]!"
    exit 1
    ;;

  esac

  if [ ! -f "$requirementsPath" ]; then
    echo "Python requirements file [$requirementsPath] does not exist!"
    exit 1
  fi

  if [ ! -d "$outputPath" ]; then
    echo "Output directory [$outputPath] does not exist! Please create it first."
    exit 1
  fi
}

function get_conda_name() {
  condaName=""
  case $pythonVersion in
  2.7)
    condaName="py27"
    ;;
  3.5)
    condaName="py35"
    ;;
  3.6)
    condaName="py36"
    ;;
  3.7)
    condaName="py37"
    ;;
  esac
  echo "$condaName"
}

function get_image_version() {
  imageVersion=""
  case $pythonVersion in
  2.7)
    imageVersion=$imagePrefix":py27-v0.1"
    ;;
  3.5)
    imageVersion=$imagePrefix":py35-v0.1"
    ;;
  3.6)
    imageVersion=$imagePrefix":py36-v0.1"
    ;;
  3.7)
    imageVersion=$imagePrefix":py37-v0.1"
    ;;
  esac
  echo "$imageVersion"
}

function handle_py() {
  reqTxtNameInContainer="req_tmp.txt"
  if [ "$cleanMode" -eq "1" ]; then
    image=$imagePrefix":base-v0.1"
    condaEnv=$(get_conda_name)
    scriptStr="source ~/.bashrc && conda create -y -n $condaEnv python=$pythonVersion && source activate $condaEnv && conda install -y pip && pip install --no-cache-dir conda-pack && pip install -r /$reqTxtNameInContainer && conda pack -n $condaEnv"
  else
    image=$(get_image_version)
    condaEnv=$(get_conda_name)
    scriptStr="source ~/.bashrc && source activate $condaEnv && pip install -r /$reqTxtNameInContainer && conda pack -n $condaEnv"
  fi

  containerName="py-test"

  runningInstanceCnt=$(docker ps -a | grep $containerName | wc -l)
  if [ "$runningInstanceCnt" -ne "0" ]; then
    docker rm -f $containerName
  fi

  docker pull "$image"
  docker run -itd --name $containerName "$image"
  docker cp "$requirementsPath" "$containerName:/$reqTxtNameInContainer"
  docker exec $containerName /bin/bash -c "$scriptStr"
  docker cp "$containerName:/$condaEnv.tar.gz" "$outputPath"
  docker stop $containerName
  docker rm $containerName
}

pythonVersion=""
requirementsPath=""
outputPath=""
cleanMode=0

while getopts "p:r:t:ch" opt; do
  case $opt in
  p)
    pythonVersion=$OPTARG
    ;;
  r)
    requirementsPath=$OPTARG
    ;;
  t)
    outputPath=$OPTARG
    ;;
  c)
    cleanMode=1
    ;;
  h)
    usage_hint
    exit 0
    ;;
  *)
    usage_hint
    exit 1
    ;;
  esac
done

check_parameters
imagePrefix="registry.cn-hangzhou.aliyuncs.com/maxcompute-cupid-pyspark/cupid-pyspark"
handle_py
