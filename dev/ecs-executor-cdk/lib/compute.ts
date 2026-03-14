import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as path from 'path';
import { Construct } from 'constructs';

export function createEc2(
  scope: Construct,
  vpc: ec2.IVpc,
  sg: ec2.ISecurityGroup,
  role: iam.IRole,
): ec2.Instance {
  // Upload ec2_scripts/ as an S3 asset — CDK zips and uploads during deploy
  const scriptsAsset = new s3assets.Asset(scope, 'AirflowScripts', {
    path: path.join(__dirname, '..', 'ec2_scripts'),
  });
  scriptsAsset.grantRead(role);

  const userData = ec2.UserData.forLinux();
  userData.addCommands(
    '#!/bin/bash',
    'set -e',
    'dnf update -y',
    'dnf install -y git docker unzip nodejs tmux jq',
    '',
    '# AWS CLI v2',
    'curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip',
    'unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install',
    '',
    '# Python 3.12 + uv',
    'dnf install -y python3.12 python3.12-pip',
    'pip3.12 install uv',
    '',
    '# psql client',
    'dnf install -y postgresql15',
    '',
    '# Docker',
    'systemctl enable docker',
    'systemctl start docker',
    'usermod -aG docker ec2-user',
    '',
    '# Deploy scripts from S3 asset',
    'mkdir -p /opt/airflow-scripts',
    `aws s3 cp s3://${scriptsAsset.s3BucketName}/${scriptsAsset.s3ObjectKey} /tmp/scripts.zip`,
    'unzip -o /tmp/scripts.zip -d /opt/airflow-scripts/',
    'chmod +x /opt/airflow-scripts/*.sh',
    'chown -R ec2-user:ec2-user /opt/airflow-scripts',
    '',
    '# Source CLI helpers in ec2-user bashrc',
    'echo "source /opt/airflow-scripts/airflow-cli-helpers.sh" >> /home/ec2-user/.bashrc',
  );

  const instance = new ec2.Instance(scope, 'Ec2', {
    vpc,
    vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.LARGE),
    machineImage: ec2.MachineImage.latestAmazonLinux2023(),
    securityGroup: sg,
    role,
    userData,
    // No key pair — shell access via SSM Session Manager only
    blockDevices: [
      {
        deviceName: '/dev/xvda',
        volume: ec2.BlockDeviceVolume.ebs(50, {
          volumeType: ec2.EbsDeviceVolumeType.GP3,
          deleteOnTermination: true,
        }),
      },
    ],
  });

  return instance;
}
