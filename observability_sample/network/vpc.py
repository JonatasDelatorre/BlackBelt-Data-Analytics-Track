from constructs import Construct
from aws_cdk import (
    aws_ec2 as _ec2,
)

class DatalakeVpc(Construct):

    @property
    def get_vpc(self):
        return self.vpc

    def __init__(self, scope: Construct, id: str, **kwargs):

        super().__init__(scope, id, **kwargs)

        self.vpc = _ec2.Vpc(
            self, "DatalakeVpc",
            cidr= "10.0.0.0/16",
            max_azs=2
        )

    # vpc = aws_ec2.Vpc(
    #     self,
    #     id='prod_vpc',
    #     cidr='10.199.0.0/16',
    #     enable_dns_hostnames=False,
    #     enable_dns_support=True,
    #     nat_gateways=1,
    #     max_azs=2,
    #     subnet_configuration=[
    #         aws_ec2.SubnetConfiguration(
    #             cidr_mask=24,
    #             name='public',
    #             subnet_type=aws_ec2.SubnetType.PUBLIC
    #         ),
    #         aws_ec2.SubnetConfiguration(
    #             cidr_mask=20,
    #             name='application',
    #             subnet_type=aws_ec2.SubnetType.PRIVATE
    #         )
    #     ]
    # )