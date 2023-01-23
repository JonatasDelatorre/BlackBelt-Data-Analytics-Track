from constructs import Construct
from aws_cdk import (
    aws_ec2 as _ec2,
)


class DatalakeSGs(Construct):

    @property
    def get_engine_workspace_sg(self):
        return self.engine_sg, self.workspace_sg, self.opensearch_sg

    def __init__(self, scope: Construct, id: str, vpc: _ec2.Vpc, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.engine_sg = _ec2.SecurityGroup(
            self, "engine-sg",
            vpc=vpc,
            allow_all_outbound=True,
        )

        self.workspace_sg = _ec2.SecurityGroup(
            self, "workspace-sg",
            vpc=vpc,
            allow_all_outbound=True,
        )

        _ec2.CfnSecurityGroupIngress(
            self, "engine-sg-ingress",
            ip_protocol="tcp",
            from_port=18888,
            to_port=18888,
            group_id=self.engine_sg.security_group_id,
            source_security_group_id=self.workspace_sg.security_group_id
        )

        _ec2.CfnSecurityGroupEgress(
            self, "workspace-sg-egress-engine",
            ip_protocol="tcp",
            from_port=18888,
            to_port=18888,
            group_id=self.workspace_sg.security_group_id,
            destination_security_group_id=self.engine_sg.security_group_id
        )

        _ec2.CfnSecurityGroupEgress(
            self, "workspace-sg-egress-443",
            ip_protocol="tcp",
            from_port=443,
            to_port=443,
            group_id=self.workspace_sg.security_group_id,
            cidr_ip="0.0.0.0/0"
        )

        self.opensearch_sg = _ec2.SecurityGroup(
            self, 'OpenSearchDomainSG',
            vpc=vpc,
            allow_all_outbound=True,
            security_group_name='OpenSearchSecGrpMonitoring')

        self.opensearch_sg.add_ingress_rule(_ec2.Peer.any_ipv4(), _ec2.Port.tcp(80))
        self.opensearch_sg.add_ingress_rule(_ec2.Peer.any_ipv4(), _ec2.Port.tcp(443))
