#!/bin//bash -xe
echo 'Python version: ' $(python3 --version)
echo 'Installing python dependencies'
sudo python3 -m pip install numpy
echo '... complete.'
