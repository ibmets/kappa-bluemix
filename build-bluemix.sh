#!/usr/bin/env bash

rm -fr target/bluemix
gradle -b build-bluemix.gradle build war
