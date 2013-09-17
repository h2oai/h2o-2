#/bin/sh
SH2JU=~/shell2junit/sh2ju.sh
echo "Checking that sh2ju.sh exists in the right place"
if [ -f $SH2JU ]
then
    echo "$SH2JU exists."
else
    # http://code.google.com/p/shell2junit
    # use in jenkins:
    # http://manolocarrasco.blogspot.com/2010/02/hudson-publish-bach.html
    pushd ~
    wget http://shell2junit.googlecode.com/files/shell2junit-1.0.0.zip .
    unzip shell2junit-1.0.0.zip 
    ls -lt shell2junit/sh2ju_example.sh  
    ls -lt shell2junit/sh2ju.sh    
    popd

    if [ -f $SH2JU ]
    then
        echo "$SH2JU exists."
    fi
fi

#### Include the library
source $SH2JU

#### Clean old reports
juLogClean
#### Success command
juLog  -name=myTrueCommand true
#### Failure
juLog  -name=myFalseCommand false
#### Sleep
juLog  -name=mySleepCommand sleep 5
#### The test fails because the word 'world' is found in command output
juLog  -name=myErrorCommand -ierror=world   echo Hello World
#### A sql command
juLog  -name=myLsCommand /bin/ls

#### A call to a customized method
myCmd() {
   ls -l $*
   return 0
}
juLog  -name=myCustomizedMethod myCmd '*.sh'

