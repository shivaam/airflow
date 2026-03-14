import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

export interface StorageResources {
  db: rds.DatabaseInstance;
  dbSecret: secretsmanager.ISecret;
  logBucket: s3.Bucket;
  dagBucket: s3.Bucket;
  ecrRepo: ecr.Repository;
}

export function createStorage(
  scope: Construct,
  vpc: ec2.IVpc,
  dbSg: ec2.ISecurityGroup,
): StorageResources {
  const bucket = new s3.Bucket(scope, 'LogBucket', {
    bucketName: `airflow-ecs-logs-${cdk.Stack.of(scope).account}-${cdk.Stack.of(scope).region}`,
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    autoDeleteObjects: true,
    versioned: false,
  });

  const dagBucket = new s3.Bucket(scope, 'DagBucket', {
    bucketName: `airflow-ecs-dags-${cdk.Stack.of(scope).account}-${cdk.Stack.of(scope).region}`,
    blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    autoDeleteObjects: true,
    versioned: false,
  });

  const db = new rds.DatabaseInstance(scope, 'Db', {
    engine: rds.DatabaseInstanceEngine.postgres({
      version: rds.PostgresEngineVersion.VER_16,
    }),
    instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM),
    vpc,
    vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    securityGroups: [dbSg],
    databaseName: 'airflow_db',
    multiAz: false,
    storageEncrypted: true,
    allocatedStorage: 20,
    deletionProtection: false,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    backupRetention: cdk.Duration.days(0), // no backups for test env
  });

  const ecrRepo = new ecr.Repository(scope, 'EcrRepo', {
    repositoryName: 'airflow-ecs-worker',
    imageTagMutability: ecr.TagMutability.MUTABLE,
    imageScanOnPush: false,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    emptyOnDelete: true,
    lifecycleRules: [
      {
        maxImageCount: 5,
        description: 'Keep last 5 images',
      },
    ],
  });

  return { db, dbSecret: db.secret!, logBucket: bucket, dagBucket, ecrRepo };
}
