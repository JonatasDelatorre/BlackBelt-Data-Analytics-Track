import aws_cdk as core
import aws_cdk.assertions as assertions

from observability_sample.observability_sample_stack import ObservabilitySampleStack

# example tests. To run these tests, uncomment this file along with the example
# resource in observability_sample/observability_sample_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ObservabilitySampleStack(app, "observability-sample")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
