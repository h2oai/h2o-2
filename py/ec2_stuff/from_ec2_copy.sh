
# scp -r -i /home/hduser/.ec2/keys/mrjenkins_test.pem hduser@ec2-184-73-55-110.compute-1.amazonaws.com:/home/hduser/.ec2 dot_ec2_from_ec2

# scp -r -i /home/kevin/.ec2/keys/0xdata_Big.pem kevin@ec2-50-18-147-48.us-west-1.compute.amazonaws.com:/home/kevin/.ec2 dot_ec2_from_ec2
# scp -r -i /home/kevin/.ec2/keys/0xdata_Big.pem 

scp -i ~/.ec2/keys/mrjenkins_test.pem hduser@23.21.237.69:/home/kevin/h2o/py/testdir_single_jvm/*.jpg  .

