terraform {
  required_version = ">= 1.6"

  backend "s3" {
    bucket = "finstreami-terraform-state"
    key    = "prod/terraform.tfstate"
    region = "us-east-1"
  }
}

module "finstreami" {
  source = "../../"

  aws_region   = var.aws_region
  environment  = "prod"
  project_name = var.project_name
  cluster_name = var.cluster_name
  vpc_cidr     = var.vpc_cidr
}
