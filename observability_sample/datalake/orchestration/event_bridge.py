from constructs import Construct
from aws_cdk import (
    aws_stepfunctions as _stf,
    aws_events_targets as _events_targets,
    aws_events as _events,
    aws_s3 as _s3,
    aws_stepfunctions as _stf,

)


class StfEventBridgeS3(Construct):

    def __init__(self,
                 scope: Construct,
                 id: str,
                 raw_bucket: _s3.Bucket,
                 state_machine: _stf.StateMachine,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        event_rule = _events.Rule(
            self, "Event invoke stf from s3",
            event_pattern=_events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {
                        "name": [raw_bucket.bucket_name]
                    }
                }
            )
        )

        event_rule.add_target(_events_targets.SfnStateMachine(machine=state_machine))
