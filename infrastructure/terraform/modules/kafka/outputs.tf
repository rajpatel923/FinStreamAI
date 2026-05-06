output "bootstrap_brokers" {
  description = "MSK plaintext bootstrap broker endpoints"
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "bootstrap_brokers_tls" {
  description = "MSK TLS bootstrap broker endpoints"
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
}

output "cluster_arn" {
  value = aws_msk_cluster.main.arn
}
