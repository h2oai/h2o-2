
echo "jenkins causes these, since he dispatches h2o as either. So he needs to clean up"
# the 0xdiag ones may be shared with other people doing python tests, so this will whack them

#****************************************************************************
# got the private id from 180
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.161 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.162 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.163 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.164 rm -f -r /home/0xcustomer/ice*

# dead now
# ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.171 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.172 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.173 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.174 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.175 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.176 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.177 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.178 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.179 rm -f -r /home/0xcustomer/ice*
ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.180 rm -f -r /home/0xcustomer/ice*

#****************************************************************************
# got the private id from 180
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.161 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.162 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.163 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.164 rm -f -r /home/0xdiag/ice*

ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.171 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.172 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.173 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.174 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.175 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.176 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.177 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.178 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.179 rm -f -r /home/0xdiag/ice*
ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.180 rm -f -r /home/0xdiag/ice*
