output "cluster_name" { value = aws_eks_cluster.platform.name }
output "private_endpoint" { value = aws_eks_cluster.platform.endpoint }
output "image_repository" { value = aws_ecr_repository.platform.repository_url }
