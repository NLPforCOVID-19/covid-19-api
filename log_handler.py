import json
import os
from typing import List

TOPIC_CHECK_LOG = 'category_check.txt'
FEEDBACK_LOG = 'feedback.txt'
PAGE_NUMBER_LOG = 'update.txt'


class LogHandler:

    def __init__(self, log_dir: str):
        self.log_dir = log_dir

    def extend_topic_check_log(self, lines: List[str]):
        self.extend_log(os.path.join(self.log_dir, TOPIC_CHECK_LOG), lines)

    def extend_feedback_log(self, lines: List[str]):
        self.extend_log(os.path.join(self.log_dir, FEEDBACK_LOG), lines)

    def extend_page_number_log(self, lines: List[str]):
        self.extend_log(os.path.join(self.log_dir, PAGE_NUMBER_LOG), lines)

    @staticmethod
    def extend_log(path: str, lines: List[str]):
        with open(path, 'a') as f:
            for line in lines:
                f.write(line + '\n')

    def find_topic_check_log(self, url: str):
        path = os.path.join(self.log_dir, TOPIC_CHECK_LOG)
        with open(path) as f:
            for line in reversed(f.readlines()):
                if line.strip() == '':
                    break
                edited_info = json.loads(line)
                if edited_info.get('url', '') == url:
                    edited_info['is_checked'] = 1
                    return edited_info
        return {'url': url, 'is_checked': 0}

    def iterate_topic_check_log(self):
        path = os.path.join(self.log_dir, TOPIC_CHECK_LOG)
        with open(path) as f:
            for line in f:
                if line.strip():
                    yield line
