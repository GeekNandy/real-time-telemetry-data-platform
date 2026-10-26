variable "name" {
  type    = string
  default = "real-time-telemetry"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,35}$", var.name))
    error_message = "Use a short lowercase project name."
  }
}
variable "region" { type = string }
variable "private_subnet_ids" {
  type = list(string)
  validation {
    condition     = length(distinct(var.private_subnet_ids)) >= 2
    error_message = "Provide private subnets in at least two availability zones."
  }
}
variable "kubernetes_version" {
  type = string
  validation {
    condition     = can(regex("^1\\.[0-9]+$", var.kubernetes_version))
    error_message = "Explicitly choose an EKS-supported Kubernetes minor version."
  }
}
variable "administrator_role_arn" {
  type = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/.+$", var.administrator_role_arn))
    error_message = "Use an existing account role, not an IAM user or account root."
  }
}
variable "ebs_csi_addon_version" { type = string }
variable "oidc_thumbprints" {
  type = list(string)
  validation {
    condition     = length(var.oidc_thumbprints) > 0 && alltrue([for value in var.oidc_thumbprints : can(regex("^[a-fA-F0-9]{40}$", value))])
    error_message = "Supply verified SHA1 OIDC CA certificate thumbprints."
  }
}
variable "node_instance_type" {
  type    = string
  default = "m7i.large"
}
variable "node_count" {
  type    = number
  default = 2
  validation {
    condition     = var.node_count >= 2 && var.node_count <= 10 && floor(var.node_count) == var.node_count
    error_message = "Use between two and ten nodes in this reference environment."
  }
}
variable "vpc_cni_addon_version" { type = string }
