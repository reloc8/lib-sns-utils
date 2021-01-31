import boto3
import logging

from dataclasses import dataclass
from typing import AnyStr, Dict


@dataclass
class AccountMetadata:

    account: AnyStr
    region: AnyStr


@dataclass(init=False)
class SnsUtils:

    logger: logging.Logger

    def __init__(self, logger: logging.Logger = logging.getLogger()):

        self.logger = logger
        self.client = boto3.client('sns')
        self.account_meta = AccountMetadata(
            account=boto3.client('sts').get_caller_identity()['Account'],
            region=self.client.meta.region_name
        )

    def send_message(self,
                     message: AnyStr,
                     topic_name: AnyStr,
                     attributes: Dict[AnyStr, AnyStr] = None) -> bool:
        """Sends a message to a SNS topic

        :param message:     Message to send
        :param topic_name:  Name of the topic
        :param attributes:  Optional message attributes
        :return:            True if message was sent
        """

        if attributes is not None:
            attributes = {key: dict(DataType='String', StringValue=attributes[key]) for key in attributes.keys()}
        else:
            attributes = {}

        region = self.account_meta.region
        account = self.account_meta.account
        status_code = self.client.publish(
            TopicArn=f'arn:aws:sns:{region}:{account}:{topic_name}',
            Message=message,
            MessageAttributes=attributes
        )['ResponseMetadata']['HTTPStatusCode']

        return 200 <= status_code <= 208
