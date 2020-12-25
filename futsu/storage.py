import os
import lazy_import
lazy = {}
lazy['fgcpstorage'] = lazy_import.lazy_module('futsu.gcp.storage')
lazy['gcstorage'] = lazy_import.lazy_module('google.cloud.storage')
lazy['fs3'] = lazy_import.lazy_module('futsu.aws.s3')
lazy['ffs'] = lazy_import.lazy_module('futsu.fs')
lazy['urllib_request'] = lazy_import.lazy_module('urllib.request')
lazy['shutil'] = lazy_import.lazy_module('shutil')


def local_to_path(dst, src):
    if lazy['fgcpstorage'].is_blob_path(dst):
        gcs_client = lazy['gcstorage'].client.Client()
        lazy['fgcpstorage'].file_to_blob(dst, src, gcs_client)
        return
    if lazy['fs3'].is_blob_path(dst):
        client = lazy['fs3'].create_client()
        lazy['fs3'].file_to_blob(dst, src, client)
        return
    lazy['ffs'].cp(dst, src)


def path_to_local(dst, src):
    if lazy['fgcpstorage'].is_blob_path(src):
        gcs_client = lazy['gcstorage'].client.Client()
        lazy['fgcpstorage'].blob_to_file(dst, src, gcs_client)
        return
    if lazy['fs3'].is_blob_path(src):
        client = lazy['fs3'].create_client()
        lazy['fs3'].blob_to_file(dst, src, client)
        return
    if src.startswith('https://') or src.startswith('http://'):
        with lazy['urllib_request'].urlopen(src) as w_in, open(dst, 'wb') as f_out:
            lazy['shutil'].copyfileobj(w_in, f_out)
        return
    lazy['ffs'].cp(dst, src)


def bytes_to_path(dst, bytes):
    if lazy['fgcpstorage'].is_blob_path(dst):
        gcs_client = lazy['gcstorage'].client.Client()
        lazy['fgcpstorage'].bytes_to_blob(dst, bytes, gcs_client)
        return
    if lazy['fs3'].is_blob_path(dst):
        client = lazy['fs3'].create_client()
        lazy['fs3'].bytes_to_blob(dst, bytes, client)
        return
    lazy['ffs'].bytes_to_file(dst, bytes)


def path_to_bytes(src):
    if lazy['fgcpstorage'].is_blob_path(src):
        gcs_client = lazy['gcstorage'].client.Client()
        return lazy['fgcpstorage'].blob_to_bytes(src, gcs_client)
    if lazy['fs3'].is_blob_path(src):
        client = lazy['fs3'].create_client()
        return lazy['fs3'].blob_to_bytes(src, client)
    if src.startswith('https://') or src.startswith('http://'):
        with lazy['urllib_request'].urlopen(src) as w_in:
            return w_in.read()
    return lazy['ffs'].file_to_bytes(src)


def is_blob_exist(p):
    if lazy['fgcpstorage'].is_blob_path(p):
        gcs_client = lazy['gcstorage'].client.Client()
        return lazy['fgcpstorage'].is_blob_exist(p, gcs_client)
    if lazy['fs3'].is_blob_path(p):
        client = lazy['fs3'].create_client()
        return lazy['fs3'].is_blob_exist(p, client)
    return os.path.isfile(p)


def rm(p):
    if lazy['fgcpstorage'].is_blob_path(p):
        gcs_client = lazy['gcstorage'].client.Client()
        return lazy['fgcpstorage'].blob_rm(p, gcs_client)
    if lazy['fs3'].is_blob_path(p):
        client = lazy['fs3'].create_client()
        return lazy['fs3'].blob_rm(p, client)
    return os.remove(p)


def find(p):
    if lazy['fgcpstorage'].is_blob_path(p):
        gcs_client = lazy['gcstorage'].client.Client()
        return lazy['fgcpstorage'].find_blob_itr(p, gcs_client)
    if lazy['fs3'].is_blob_path(p):
        client = lazy['fs3'].create_client()
        return lazy['fs3'].find_blob_itr(p, client)
    return lazy['ffs'].find_file(p)


def join(*args):
    if lazy['fgcpstorage'].is_path(args[0]):
        return lazy['fgcpstorage'].join(*args)
    if lazy['fs3'].is_path(args[0]):
        return lazy['fs3'].join(*args)
    if args[0].startswith('https://') or args[0].startswith('http://'):
        return '/'.join(args)
    return os.path.join(*args)


def split(p):
    if lazy['fgcpstorage'].is_path(p):
        return lazy['fgcpstorage'].split(p)
    if lazy['fs3'].is_path(p):
        return lazy['fs3'].split(p)
    if p.startswith('https://') or p.startswith('http://'):
        return (dirname(p), basename(p))
    return os.path.split(p)


def basename(p):
    if lazy['fgcpstorage'].is_path(p):
        return lazy['fgcpstorage'].basename(p)
    if lazy['fs3'].is_path(p):
        return lazy['fs3'].basename(p)
    if p.startswith('https://') or p.startswith('http://'):
        return p[p.rindex('/')+1:]
    return os.path.basename(p)


def dirname(p):
    if lazy['fgcpstorage'].is_path(p):
        return lazy['fgcpstorage'].dirname(p)
    if lazy['fs3'].is_path(p):
        return lazy['fs3'].dirname(p)
    if p.startswith('https://') or p.startswith('http://'):
        return p[:p.rindex('/')]
    return os.path.dirname(p)


def rmtree(p):
    if lazy['fgcpstorage'].is_path(p):
        gcs_client = lazy['gcstorage'].client.Client()
        lazy['fgcpstorage'].rmtree(p, gcs_client)
    if lazy['fs3'].is_path(p):
        client = lazy['fs3'].create_client()
        lazy['fs3'].rmtree(p, client)
    if p.startswith('https://') or p.startswith('http://'):
        raise Exception('CVALHPEH http(s) not support rmtree')
    lazy['shutil'].rmtree(p, ignore_errors=True)
