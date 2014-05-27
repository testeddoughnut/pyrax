#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import unittest

from six import StringIO

from mock import patch
from mock import MagicMock as Mock

import pyrax
import pyrax.object_storage
from pyrax.object_storage import get_file_size
from pyrax.object_storage import _massage_metakeys
from pyrax.object_storage import _validate_file_or_path
from pyrax.object_storage import Container
from pyrax.object_storage import StorageClient
from pyrax.object_storage import StorageObject
import pyrax.exceptions as exc
import pyrax.utils as utils

import pyrax.fakes as fakes



class ObjectStorageTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ObjectStorageTest, self).__init__(*args, **kwargs)

    def setUp(self):
        self.client = fakes.FakeStorageClient()
        self.container = fakes.FakeContainer()

    def tearDown(self):
        pass

    def test_massage_metakeys(self):
        prefix = "ABC-"
        orig = {"ABC-yyy": "ok", "zzz": "change"}
        expected = {"ABC-yyy": "ok", "ABC-zzz": "change"}
        fixed = _massage_metakeys(orig, prefix)
        self.assertEqual(fixed, expected)

    def test_validate_file_or_path(self):
        obj_name = utils.random_unicode()
        with utils.SelfDeletingTempfile() as tmp:
            ret = _validate_file_or_path(tmp, obj_name)
        self.assertEqual(ret, obj_name)

    def test_validate_file_or_path_not_found(self):
        pth = utils.random_unicode()
        obj_name = utils.random_unicode()
        self.assertRaises(exc.FileNotFound, _validate_file_or_path, pth,
                obj_name)

    def test_validate_file_or_path_object(self):
        pth = object()
        obj_name = utils.random_unicode()
        ret = _validate_file_or_path(pth, obj_name)
        self.assertEqual(ret, obj_name)

    def test_get_file_size(self):
        sz = random.randint(42, 420)
        fobj = StringIO("x" * sz)
        ret = get_file_size(fobj)
        self.assertEqual(sz, ret)

    @patch('pyrax.object_storage.StorageObjectManager',
            new=fakes.FakeStorageObjectManager)
    def test_container_create(self):
        api = utils.random_unicode()
        mgr = fakes.FakeManager()
        mgr.api = api
        nm = utils.random_unicode()
        info = {"name": nm}
        cont = Container(mgr, info)
        self.assertEqual(cont.manager, mgr)
        self.assertEqual(cont._info, info)
        self.assertEqual(cont.name, nm)

    def test_cont_get(self):
        cont = self.container
        cont.object_manager.get = Mock()
        item = utils.random_unicode()
        cont.get(item)
        cont.object_manager.get.assert_called_once_with(item)

    def test_cont_list(self):
        cont = self.container
        cont.object_manager.list = Mock()
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = utils.random_unicode()
        cont.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        cont.object_manager.list.assert_called_once_with(marker=marker,
                limit=limit, prefix=prefix, delimiter=delimiter,
                end_marker=end_marker, return_raw=return_raw)

    def test_cont_find(self):
        cont = self.container
        cont.object_manager.find = Mock()
        key = utils.random_unicode()
        val = utils.random_unicode()
        cont.find(key=val)
        cont.object_manager.find.assert_called_once_with(key=val)

    def test_cont_findall(self):
        cont = self.container
        cont.object_manager.findall = Mock()
        key = utils.random_unicode()
        val = utils.random_unicode()
        cont.findall(key=val)
        cont.object_manager.findall.assert_called_once_with(key=val)

    def test_cont_create(self):
        cont = self.container
        cont.object_manager.create = Mock()
        fop = utils.random_unicode()
        data = utils.random_unicode()
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        ttl = utils.random_unicode()
        chunked = utils.random_unicode()
        metadata = utils.random_unicode()
        cont.create(file_or_path=fop, data=data, obj_name=obj_name,
                content_type=content_type, etag=etag,
                content_encoding=content_encoding,
                content_length=content_length, ttl=ttl, chunked=chunked,
                metadata=metadata)
        cont.object_manager.create.assert_called_once_with(file_or_path=fop,
                data=data, obj_name=obj_name, content_type=content_type,
                etag=etag, content_encoding=content_encoding,
                content_length=content_length, ttl=ttl, chunked=chunked,
                metadata=metadata)

    def test_cont_fetch(self):
        cont = self.container
        cont.object_manager.fetch = Mock()
        obj = utils.random_unicode()
        cont.fetch(obj)
        cont.object_manager.fetch.assert_called_once_with(obj,
                include_meta=False, chunk_size=None, size=None)

    def test_cont_download(self):
        cont = self.container
        cont.object_manager.download = Mock()
        obj = utils.random_unicode()
        directory = utils.random_unicode()
        structure = utils.random_unicode()
        cont.download(obj, directory, structure=structure)
        cont.object_manager.download.assert_called_once_with(obj, directory,
                structure=structure)

    def test_cont_delete_object(self):
        cont = self.container
        cont.object_manager.delete = Mock()
        obj = utils.random_unicode()
        cont.delete_object(obj)
        cont.object_manager.delete.assert_called_once_with(obj)

    def test_storage_object_id(self):
        cont = self.container
        nm = utils.random_unicode()
        sobj = StorageObject(cont.object_manager, {"name": nm})
        self.assertEqual(sobj.name, nm)
        self.assertEqual(sobj.id, nm)

    def test_storage_object_mgr_name(self):
        cont = self.container
        om = cont.object_manager
        self.assertEqual(om.name, om.uri_base)

    def test_storage_object_mgr_list_raw(self):
        cont = self.container
        om = cont.object_manager
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = utils.random_unicode()
        fake_resp = utils.random_unicode()
        fake_resp_body = utils.random_unicode()
        om.api.method_get = Mock(return_value=(fake_resp, fake_resp_body))
        ret = om.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        self.assertEqual(ret, fake_resp_body)

    def test_storage_object_mgr_list_obj(self):
        cont = self.container
        om = cont.object_manager
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = False
        fake_resp = utils.random_unicode()
        nm = utils.random_unicode()
        fake_resp_body = [{"name": nm}]
        om.api.method_get = Mock(return_value=(fake_resp, fake_resp_body))
        ret = om.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 1)
        obj = ret[0]
        self.assertEqual(obj.name, nm)

    def test_storage_object_mgr_list_subdir(self):
        cont = self.container
        om = cont.object_manager
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = False
        fake_resp = utils.random_unicode()
        sd = utils.random_unicode()
        nm = utils.random_unicode()
        fake_resp_body = [{"subdir": sd, "name": nm}]
        om.api.method_get = Mock(return_value=(fake_resp, fake_resp_body))
        ret = om.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 1)
        obj = ret[0]
        self.assertEqual(obj.name, sd)

    def test_storage_object_mgr_get(self):
        cont = self.container
        om = cont.object_manager
        obj = utils.random_unicode()
        contlen = random.randint(100, 1000)
        conttype = utils.random_unicode()
        etag = utils.random_unicode()
        lastmod = utils.random_unicode()
        fake_resp = fakes.FakeResponse()
        fake_resp.headers = {"content-length": contlen,
                "content-type": conttype,
                "etag": etag,
                "last-modified": lastmod,
                }
        om.api.method_head = Mock(return_value=(fake_resp, None))
        ret = om.get(obj)
        self.assertEqual(ret.name, obj)
        self.assertEqual(ret.bytes, contlen)
        self.assertEqual(ret.content_type, conttype)
        self.assertEqual(ret.hash, etag)
        self.assertEqual(ret.last_modified, lastmod)

    def test_storage_object_mgr_create_empty(self):
        cont = self.container
        om = cont.object_manager
        self.assertRaises(exc.NoContentSpecified, om.create)



if __name__ == "__main__":
    unittest.main()
