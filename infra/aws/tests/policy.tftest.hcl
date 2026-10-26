mock_provider "aws" {}
variables {
  region                 = "eu-central-1"
  private_subnet_ids     = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
  kubernetes_version     = "1.33"
  administrator_role_arn = "arn:aws:iam::123456789012:role/platform-admin"
  vpc_cni_addon_version  = "v1.20.0-eksbuild.1"
  ebs_csi_addon_version  = "v1.44.0-eksbuild.1"
  oidc_thumbprints       = ["0000000000000000000000000000000000000000"]
}
run "private_cluster_and_durable_nodes" {
  command = plan
  assert {
    condition     = aws_eks_cluster.platform.vpc_config[0].endpoint_private_access && !aws_eks_cluster.platform.vpc_config[0].endpoint_public_access
    error_message = "The Kubernetes API must not be public."
  }
  assert {
    condition     = aws_launch_template.nodes.metadata_options[0].http_tokens == "required" && aws_launch_template.nodes.metadata_options[0].http_put_response_hop_limit == 1
    error_message = "Require IMDSv2 and prevent application pods from reaching node metadata."
  }
  assert {
    condition     = one(aws_launch_template.nodes.block_device_mappings).ebs[0].encrypted
    error_message = "Node volumes must be encrypted."
  }
  assert {
    condition     = aws_ecr_repository.platform.image_tag_mutability == "IMMUTABLE" && !aws_ecr_repository.platform.force_delete
    error_message = "Preserve immutable published image identities."
  }
  assert {
    condition     = !aws_eks_cluster.platform.access_config[0].bootstrap_cluster_creator_admin_permissions
    error_message = "Administrative access must be an explicit role entry."
  }
}
run "reject_single_subnet" {
  command = plan
  variables { private_subnet_ids = ["subnet-0123456789abcdef0"] }
  expect_failures = [var.private_subnet_ids]
}
