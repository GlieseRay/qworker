# -*- coding:utf-8 -*-
'''
rackspace cloudfiles uploader

Created on Oct 30, 2012
@author: ray
'''
import os
import logging
import traceback
import cloudfiles

import qworker

logging.basicConfig(level=logging.DEBUG)


class CloudFileStorage(object):

    """ A wrapper for CloudFiles """

    def __init__(self, user, api_key, container_name):
        """
        @param user: user name
        @param api_key: api_key of CloudFiles
        @param container_name: name of container
        """
        self._conn = cloudfiles.Connection(user, api_key)
        self._container = self._conn.create_container(container_name)

    def put(self, name, data):
        obj = self._container.create_object(name)
        obj.write(data, verify=True)

    def put_many(self, name_data_dict):
        for name, data in name_data_dict:
            self.put(name, data)

    def get(self, name):
        obj = self._container.get_object(name)
        return obj.read()

    def get_many(self, names):
        for name in names:
            return name, self.get(name)

    def has(self, name):
        return bool(self._container.list_objects(prefix=name, limit=1))

    def has_many(self, names):
        return all(self.has(name) for name in names)

    def purge(self):
        names = self._container.list_objects()
        for name in names:
            self._container.delete_object(name)

    def count(self):
        return self._container.object_count

    def close(self):
        pass


class DirWalker(qworker.Producer):

    def __init__(self, dirpath):
        if not os.path.exists(dirpath):
            raise ValueError('%s not found' % dirpath)
        self._dirpath = dirpath

    def items(self):
        dirpath = os.path.abspath(self._dirpath)
        dirname = os.path.split(dirpath)[1]

        count = self._count(dirpath)
        logging.info('%d object to be uploaded.' % count)

        idx = 0
        for root, __dirs, files in os.walk(dirpath):
            for filename in files:
                filepath = os.path.join(root, filename)
                relpath = os.path.relpath(filepath, self._dirpath)
                abspath = os.path.abspath(filepath)

                idx += 1
                tag = '%d/%d' % (idx, count)
                name = os.path.join(dirname, relpath)
                yield tag, name, abspath

    def _count(self, dirpath):
        n = 0
        for __root, __dirs, files in os.walk(dirpath):
            for __file in files:
                n += 1
        return n


class DirUploader(qworker.Consumer):

    def __init__(self, user, api_key, container_name, overwrite=False):
        self._cloudfile = CloudFileStorage(user=user,
                                           api_key=api_key,
                                           container_name=container_name
                                           )
        self._overwrite = overwrite

    def consume(self, task):
        tag, name, filepath = task
        with open(filepath, 'rb') as fp:
            data = fp.read()

        for _i in range(10):
            try:
                if not self._overwrite and self._cloudfile.has(name):
                    logging.info('%s: %s already exists.' % (tag, name))
                else:
                    self._cloudfile.put(name, data)
                    logging.info('%s: %s uploaded.' % (tag, name))
                break
            except Exception as e:
                logging.info('retrying...%s(%s)' % (e.__class__, str(e)))


class DirectoryUploader(object):

    def __init__(self, dirpath, user, api_key, container_name,
                 overwrite=False, worker_num=1):
        self._dirpath = dirpath
        self._user = user
        self._api_key = api_key
        self._container_name = container_name
        self._overwrite = overwrite
        self._work_num = worker_num

    def upload(self):
        producer = DirWalker(self._dirpath)
        consumers = list(DirUploader(user=self._user,
                            api_key=self._api_key,
                            container_name=self._container_name,
                            overwrite=self._overwrite
                            )
                         for _i in range(self._work_num)
                         )
        with qworker.Mothership(producer, consumers) as m:
            m.start()

    def purge(self):
        container = CloudFileStorage(self._user,
                                     self._api_key,
                                     self._container_name
                                     )
        container.purge()
        logging.info('container %s purged!' % self._container_name)

    def check(self):
        pass



if __name__ == '__main__':
    m = DirectoryUploader(dirpath='/tmp',
                          user='raymond851102',
                          api_key='8f67ace666112bd0c699528b07d5866e',
                          container_name='test'
                          )
    m.upload()
