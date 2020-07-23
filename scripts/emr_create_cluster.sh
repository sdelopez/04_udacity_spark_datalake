aws emr create-cluster \
--name 'udacity-cluster' \
--applications Name=Spark Name=Zeppelin \
--ec2-attributes '{"KeyName":"spark-cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-5520687e","EmrManagedSlaveSecurityGroup":"sg-06bf6ec0d6722ee5c","EmrManagedMasterSecurityGroup":"sg-0edbb378883ce9279"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.29.0 \
--log-uri 's3n://aws-logs-964592634597-us-west-2/elasticmapreduce/' \
--instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"}]' \
--configurations '[{"Classification":"spark","Properties":{}}]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-west-2