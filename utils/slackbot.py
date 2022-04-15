import os
import sys
import json
import time
import typing
import calendar
import requests


class Slack:
    url = os.getenv('SLACK_URL', '')

    def __init__(self, title: str) -> None:
        timestamp: str = str(calendar.timegm(time.gmtime()))
        self.title: str = f'{title} - {timestamp}'

        if self.url is None or self.url == '':
            raise Exception('No Slack URL!')

    def __prepare_message_data(self, message: str) -> typing.Dict[str, typing.Any]:
        slack_data: typing.Dict[str, typing.Any] = {
            'username': 'NotificationBot',
            'icon_emoji': ':satellite:',
            'attachments': [
                {
                    'color': '#9733EE',
                    'fields': [
                        {
                            'title': self.title,
                            'value': message,
                            'short': 'false',
                        }
                    ]
                }
            ]
        }
        byte_length = str(sys.getsizeof(slack_data))
        self.headers = {'Content-Type': 'application/json', 'Content-Length': byte_length}

        return slack_data

    def send(self, message: str):
        data = self.__prepare_message_data(message=message)
        response = requests.post(
            self.url,
            data=json.dumps(data),
            headers=self.headers,
        )
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
