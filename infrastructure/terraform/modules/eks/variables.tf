variable "name_prefix" {
  description = "Name prefix for EKS resources"
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for EKS nodes"
  type        = list(string)
}

variable "general_instance_type" {
  description = "Instance type for general workload node group"
  type        = string
  default     = "t3.medium"
}

variable "general_desired_size" {
  type    = number
  default = 2
}

variable "general_min_size" {
  type    = number
  default = 1
}

variable "general_max_size" {
  type    = number
  default = 5
}

variable "ml_instance_type" {
  description = "Instance type for ML workload node group"
  type        = string
  default     = "g4dn.xlarge"
}

variable "ml_desired_size" {
  type    = number
  default = 1
}

variable "ml_min_size" {
  type    = number
  default = 0
}

variable "ml_max_size" {
  type    = number
  default = 3
}
