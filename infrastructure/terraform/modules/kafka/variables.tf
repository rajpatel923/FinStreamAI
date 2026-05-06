variable "name_prefix" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "broker_storage_gb" {
  type    = number
  default = 100
}
