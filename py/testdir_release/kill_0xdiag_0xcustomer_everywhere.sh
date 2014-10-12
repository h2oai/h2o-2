
echo "Warning..other people who use the python scripts will ssh as user 0xdiag, so this will kill them"
sleep 3
# the 0xdiag ones may be shared with other people doing python tests, so this will whack them

#****************************************************************************
# got the private id from 180
# use 0xcustomer to kill 0xdiag
# ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.161 /usr/bin/pkill -u 0xcustomer
# ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.162 /usr/bin/pkill -u 0xcustomer
# ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.163 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.164 /usr/bin/pkill -u 0xcustomer

ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.171 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.172 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.173 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.174 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.175 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.176 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.177 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.178 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.179 /usr/bin/pkill -u 0xcustomer
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.180 /usr/bin/pkill -u 0xcustomer

#****************************************************************************
# got the private id from 180
# use 0xdiag to kill 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.161 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.162 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.163 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.164 /usr/bin/pkill -u 0xdiag

ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.171 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.172 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.173 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.174 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.175 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.176 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.177 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.178 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.179 /usr/bin/pkill -u 0xdiag
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@172.16.2.180 /usr/bin/pkill -u 0xdiag
