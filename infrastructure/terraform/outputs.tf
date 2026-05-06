output "vpc_id" {
  description = "ID of the VPC"
  value       = module.vpc.vpc_id
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = module.eks.cluster_endpoint
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = module.rds.db_endpoint
  sensitive   = true
}

output "msk_bootstrap_brokers" {
  description = "MSK Kafka bootstrap broker endpoints"
  value       = module.kafka.bootstrap_brokers
  sensitive   = true
}

output "redis_endpoint" {
  description = "ElastiCache Redis primary endpoint"
  value       = module.redis.primary_endpoint
  sensitive   = true
}

output "s3_data_lake_bucket" {
  description = "S3 data lake bucket name"
  value       = module.s3.bucket_name
}
