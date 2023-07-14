variable "name" {
  default     = "mychallenge"
}

variable "environment" {
  default     = "prod"
}

variable "region" {
  default     = "eu-central-1"
}

variable "cidr_block" {
  description = "CIDR block for the security group"
  default     = "0.0.0.0/0"
}

variable "db_username" {
  description = "Username for the RDS MySQL instance"
  default = "mychallenge"
}

variable "db_password" {
  description = "Password for the RDS MySQL instance"
  default = "mychallenge-secret"
}
