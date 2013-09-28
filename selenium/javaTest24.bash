#!/bin/bash

java -cp .:com/example/tests:./junit-4.11.jar:/var/lib/jenkins/selenium/selenium-server-standalone-2.35.0.jar org.junit.runner.JUnitCore com.example.tests.Test24
