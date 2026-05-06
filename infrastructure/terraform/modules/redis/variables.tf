variable "name_prefix" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "node_type" {
  type    = string
  default = "cache.r6g.large"
}

variable "num_nodes" {
  type    = number
  default = 3
}
