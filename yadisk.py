# -*- coding: utf-8 -*-
#

"""@package Yandex Disk Rest API Library
Documentation for this module.

Library for access to functions on Yandex Disk Rest API
Compatible with python 3.0+
"""
import requests
import os


class YaDNotFound(ValueError):
    pass


class YaDErrorMessage(Exception):
    pass


class YaDPayloadTooLarge(ValueError):
    pass


class YaDServerReturnCode500(Exception):
    pass


class YaDServerReturnCode503(Exception):
    pass


class YaDSpaceExhausted(ValueError):
    pass


class YaDUnknownStatus(Exception):
    pass


class YaDPreconditionFailed(ValueError):
    pass


class YaDPayloadTooLarge(OverflowError):
    pass


def _checked_data_(answer):
    status = answer.status_code
    try:
        json = answer.json()
    except ValueError:
        json = {}

    if status in (200, 201, 202):
        return json
    # if status == 201:
    #     return json
    # if status == 202:
    #     return 'accepted'
    err = json['error'] if 'error' in json.keys else ''
    description = json['description'] if 'description' in json.keys else ''
    full_desc = f'{err}: {description}'
    if status == 500:
        raise YaDServerReturnCode500(full_desc)
    elif status == 503:
        raise YaDServerReturnCode503(full_desc)
    elif answer.status_code == 404:
        raise YaDNotFound(full_desc)
    elif status == 412:
        raise YaDPreconditionFailed(full_desc)
    elif status == 413:
        raise YaDPayloadTooLarge(f'Is your file too large? > 10 GB {full_desc}')
    elif status == 507:
        raise YaDSpaceExhausted(f'End of free space! {full_desc}')
    else:
        raise YaDUnknownStatus(f'YaD REST API return {full_desc} with status {status}')


def chunk_req(file_name, block_size=1024, chunks=-1):
    with open(file_name, 'rb') as file:
        while chunks:
            data = file.read(block_size)
            if not data:
                break
            yield data


class YaDisk(object):
    def __init__(self, token):
        self.base_url = 'https://cloud-api.yandex.net:443/v1/disk/'
        self.req_headers = {'Authorization': f'OAuth {token}', }

    def ls(self, path, not_exits_create=False):
        params = {'path': f'app:/{path}'}
        command = 'resources'
        answer = requests.get(self.base_url + command,
                              headers=self.req_headers,
                              params=params)
        if answer.status_code == 404:
            if not_exits_create is True:
                paths = path.split('/')
                for i in range(0, len(paths) - 1):
                    cur_path = '/'.join(paths[0:i])
                    params = {'path': f'app:/{cur_path}'}
                    answer = requests.get(self.base_url + command,
                                          headers=self.req_headers,
                                          params=params)
                    if answer.status_code == 404:
                        self.mkdir(cur_path)
                return self.ls(path)
        return _checked_data_(answer)

    def get_link_for_upload(self, path, overwrite=True):
        params = {'path': f'{path}',
                  'overwrite': f'{str(overwrite).lower()}'}
        command = 'resources/upload/'
        answer = requests.get(self.base_url + command,
                              headers=self.req_headers,
                              params=params)

        return _checked_data_(answer)

    def put(self, path, file_name, overwrite=True):
        try:
            url = self.get_link_for_upload(f'{path}{os.path.basename(file_name)}',
                                           overwrite=overwrite)['href']
        except (YaDErrorMessage, YaDServerReturnCode500, YaDErrorMessage, YaDNotFound) as e:
            raise e
        answer = requests.put(url, data=chunk_req(file_name), stream=True)
        return _checked_data_(answer)

    def mkdir(self, path):
        params = {'path': f'app:/{path}'}
        command = 'resources'
        answer = requests.put(self.base_url + command,
                              params=params,
                              headers=self.req_headers)

        return _checked_data_(answer)

    def rm(self, path):
        params = {'path': f'app:/{path}'}
        command = 'resources'
        answer = requests.delete(self.base_url + command,
                                 params=params,
                                 headers=self.req_headers)
        return _checked_data_(answer)

    def mv(self, source_path, target_path):
        params = {'path': f'app:/{target_path}',
                  'from': f'app:/{source_path}'}
        command = 'resources/move'
        answer = requests.post(self.base_url + command,
                               params=params,
                               headers=self.req_headers)
        return _checked_data_(answer)

    def __repr__(self):
        return f'Yandex disk with {self.req_headers}'

    def __iter__(self):
        return self.ls('')
