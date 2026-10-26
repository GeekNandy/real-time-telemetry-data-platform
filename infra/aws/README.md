# Private EKS infrastructure reference

This module defines a private EKS API, explicit administrator-role access,
IMDSv2-only encrypted worker nodes, an IRSA-scoped EBS CSI role, network-policy
enforcement through VPC CNI and an immutable ECR repository. The accompanying
`deploy/kubernetes/storageclass-aws.yaml` provisions encrypted gp3 volumes with
Retain reclamation. Dependency transport checks also run inside the application.

This is infrastructure source, not evidence of a deployed AWS service. The AWS
provider could not launch in the authoring environment, so validation and mocked
policy tests remain pending on CI. Terraform formatting passed. The mock tests,
when run, do not contact AWS and cannot prove deployability, cost or availability.

Provide an existing VPC with private subnets in different AZs, required endpoints
or NAT, DNS, enough IP space, a private runner/VPN path to the API, supported EKS
and addon versions and verified OIDC CA thumbprints. The test values are synthetic
fixtures, not deployment recommendations. Workloads need HA Kafka, a compatible
registry and PostgreSQL reachable through private TLS endpoints. This module does
not provision those services or claim a complete managed-service topology.

Before a real plan, configure an encrypted remote state backend with locking and
restricted access. Do not place backend credentials, `.tfvars`, state or saved
plans in Git. Use workload identity for automation and separate plan/apply roles.

```bash
terraform init -backend=false
terraform fmt -check
terraform validate
terraform test
```

For a real environment, initialize the organization's remote backend, supply
reviewed values and inspect a saved plan, including IAM, networking, versions,
monthly cost and deletion behavior. Creating cloud resources requires a separate
explicit deployment decision. No `apply` is performed by this project's CI.
CloudWatch retention is thirty days; PVC/EBS retention intentionally leaves
resources and data behind. Include those volumes and backups in decommissioning
and privacy workflows.
