#!/bin/env python
# -*- coding:utf-8 -*-
"""
    Author : Stanley Chen
    Email : stanley_chen@sphinx.work
    Created on : 2018/2/24 上午10:08
"""
import commands
import hashlib
import os
import oss2
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s [%(filename)s][line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='w')
logger = logging.getLogger('oss_backuper')


class OssBackuper(object):

    def __init__(self, acckey_id, acckey_secret, bucket_name, endpoint, prefix, **kwargs):
        for arg in (acckey_id, acckey_secret, bucket_name, endpoint, prefix):
            if not arg:
                raise RuntimeError('Must pass (acckey_id, acckey_secret, bucket_name, endpoint), check it!')
            pass
        self.acckey_id = acckey_id
        self.acckey_secret = acckey_secret
        self.bucket_name = bucket_name
        self.endpoint = endpoint
        self.backup_prefix = prefix
        self.compress_suffix = 'tar.gz'
        # backup deadline default 30 days
        self.max_backup_day = 30
        if kwargs.get('compress_suffix', None):
            self.compress_suffix = kwargs.get('compress_suffix')
        if kwargs.get('max_backup_day', None):
            self.max_backup_day = kwargs.get('max_backup_day')
        self._oss_bucket = None

    @property
    def oss_bucket(self):
        if not self._oss_bucket:
            self._oss_bucket = oss2.Bucket(
                oss2.Auth(self.acckey_id, self.acckey_secret),
                self.endpoint, self.bucket_name
            )

        return self._oss_bucket

    @property
    def oss_key_prefix(self):
        return '-'.join([self.backup_prefix, 'backup'])

    def _get_oss_key(self, md5_hex):
        date_str = datetime.now().strftime('%Y%m%d%H%M%S')
        name = '-'.join([self.backup_prefix, date_str, md5_hex])
        return self.oss_key_prefix + '/' + name + '.' + self.compress_suffix
        pass

    def _calc_file_md5(self, filename):
        if not os.path.isfile(filename):
            raise RuntimeError('{0} is not a valid file.'.format(filename))

        md5_hash = hashlib.md5()
        with open(filename, 'rb') as f:
            while True:
                b = f.read(8096)
                if not b:
                    break
                md5_hash.update(b)
        return md5_hash.hexdigest()
        pass

    def _backup_file_to_temp(self, dir):
        if not os.path.isdir(dir):
            raise RuntimeError('Backup dir {} is not a valid dir'.format(dir))

        tmp_filename = os.tempnam()
        filename = '{filepath}.{suffix}'.format(filepath=tmp_filename, suffix=self.compress_suffix)
        command = 'tar cvzf {filename} {dir}'.format(
            **dict(filename=filename, dir=dir)
        )
        status, output = commands.getstatusoutput(command)
        if status != 0:
            raise RuntimeError('Tar file error with result code {0}'.format(status))

        md5_hex = self._calc_file_md5(filename)
        output_oss_key = self._get_oss_key(md5_hex)
        return filename, output_oss_key

    def _upload_to_oss(self, in_filename, out_oss_key):
        self.oss_bucket.put_object_from_file(out_oss_key, in_filename)
        os.remove(in_filename)
        pass

    def _remove_deadline_object(self):
        dead_line_time = datetime.now() + timedelta(days=-self.max_backup_day)
        pedding_deletes = []
        result = self.oss_bucket.list_objects(self.oss_key_prefix)
        while True:
            for obj in result.object_list:
                filename = obj.key.split(r'/')[1]
                obj_str_datetime = filename.split('-')[1]
                obj_datetime = datetime.strptime(obj_str_datetime, '%Y%m%d%H%M%S')
                if obj_datetime < dead_line_time:
                    pedding_deletes.append(obj)
                    pass
                pass
            if result.next_marker:
                result = self.oss_bucket.list_objects(self.oss_key_prefix, marker=result.next_marker)
            else:
                break

        for delete_obj in pedding_deletes:
            self.oss_bucket.delete_object(delete_obj.key)
        pass

    def backup_dir(self, dir):
        logger.debug('Begin backup {}'.format(dir))
        in_filename, out_oss_key = self._backup_file_to_temp(dir)
        self._upload_to_oss(in_filename, out_oss_key)
        self._remove_deadline_object()
        pass

    pass


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 4:
        print('You must pass {backup_prefix} {backup_dir} {max_backup_day}')
        sys.exit(-1)
        pass

    backup_prefix = sys.argv[1]
    backup_dir = sys.argv[2]
    max_backup_day = int(sys.argv[3])

    backuper = OssBackuper(
        '<access_key_id>',
        '<access_key_sec>',
        '<buctet_name>',
        '<endpoint>',
        backup_prefix,
        max_backup_day=max_backup_day
    )
    backuper.backup_dir(backup_dir)
    pass
