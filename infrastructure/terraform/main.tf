provider "aws" {
  region = var.aws_region
}

module "vpc" {
  source = "./modules/vpc"

  cidr_block = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b", "us-east-1c"]
  public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  private_subnet_cidrs = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]
}

module "eks" {
  source = "./modules/eks"

  cluster_name    = "finstreami-cluster"
  cluster_version = "1.28"
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnet_ids

  node_groups = {
    general = {
      instance_types = ["t3.medium"]
      min_size      = 2
      max_size      = 10
      desired_size  = 3
    }
    ml_workload = {
        instance_types = ["g4dn.xlarge"]
        min_size      = 1
        max_size      = 5
        desired_size  = 2
    }
  }
}

module "rds" {
  source = "./modules/rds"

  db_name           = "finstreami-postgres"
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = "db.r6g.large"
  storage_size   = 100
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnet_ids
}

module "kafka" {
    source = "./modules/kafka"
    
    cluster_name = "finstreami-kafka"
    kafka_version = "3.4.0"
    instance_type = "kafka.m5.large"
    vpc_id     = module.vpc.vpc_id
    subnet_ids = module.vpc.private_subnet_ids
}

module "redis" {
  source = "./modules/redis"

  cluster_name = "finstreami-redis"
  node_type    = "cache.t3.medium"
  num_cache_nodes = 3
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnet_ids
}

module "s3" {
  source = "./modules/s3"

  bucket_name = "finstreami-datalake"

  lifecycle_rules {
    bronze_to_ia = {
      days = 30
      storage_class = "STANDARD_IA"
    }
    silver_to_glacier = {
      days = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_db_instance" "timescaledb" {
  identifier         = "finstreami-timescaledb"
  engine             = "postgres"
  engine_version     = "15.4"
  instance_class     = "db.r6g.xlarge"
  allocated_storage  = 200
  storage_encrypted  = true
  db_name  = "timescaledb"
  username = var.db_username
  password = var.db_password
  vpc_security_group_ids = [aws_security_group.timescaledb.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  tags = {
    Name = "finstreami-timescaledb"
  }
}