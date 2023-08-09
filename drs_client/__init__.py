''' A DRS Client
Usage:
```python
import drs_client

# find info about a drs object
info = drs_client.info('drs://hostname/opaque_id')
print(info)

# open a drs object for reading
with drs_client.open('drs://hostname/opaque_id') as res:
  print(res.read())

# save to file
uri = 'drs://hostname/opaque_id'
info = drs.info(uri)
print(info['name'])
drs.dump(uri, info['name'])
```
'''
import json
import traceback
import contextlib
import urllib.request
from pathlib import Path, PurePosixPath

def _parse(uri):
  ''' Parse a uri as an easier to work with PurePosixPath
  '''
  proto, sep, path = uri.partition('://')
  assert sep, 'Not a valid URI'
  assert proto == 'drs', 'Not DRS'
  return PurePosixPath(path)

def _scheme(host):
  ''' If port is explicitly specified, fallback to http
  This isn't really defined by the DRS spec (which only uses https) but is helpful for
  debugging drs servers locally which usually aren't served over https but over http.
  '''
  if ':' in host: return 'http'
  else: return 'https'

def _info(host, opaque_id, expand=False):
  ''' GA4GH DRS v1 info API
  '''
  url = _scheme(host) + '://' + host + '/ga4gh/drs/v1/objects/' + opaque_id + ('?expand=true' if expand else '')
  try:
    with urllib.request.urlopen(url) as res:
      return json.load(res)
  except urllib.error.HTTPError as e:
    if e.code == 404:
      raise FileNotFoundError('drs://' + host + '/' + opaque_id)
    elif e.code in {401, 403}:
      raise PermissionError('drs://' + host + '/' + opaque_id)
    else:
      raise

def _access(host, opaque_id, access_id):
  ''' GA4GH DRS v1 access API
  '''
  url = _scheme(host) + '://' + host + '/ga4gh/drs/v1/objects/' + opaque_id + '/access/' + access_id
  try:
    with urllib.request.urlopen(url) as res:
      return json.load(res)
  except urllib.error.HTTPError as e:
    if e.code == 404:
      raise FileNotFoundError('drs://' + host + '/' + opaque_id + ' access_id: ' + access_id)
    elif e.code in {401, 403}:
      raise PermissionError('drs://' + host + '/' + opaque_id + ' access_id: ' + access_id)
    else:
      raise

def _flatten(path):
  ''' Any sub-object of a DRS bundle, has a unique DRS id, flatten gets the leaf path. This trick
  lets us treat bundles as subpaths but we only ever query host/opaque_id
  '''
  host, opaque_id, *subpath = path.parts
  if not subpath:
    return path, _info(host, opaque_id, expand=False)
  else:
    info = _info(host, opaque_id, expand=True)
    for i in range(len(subpath)):
      try:
        info = next(iter(item for item in info.get('contents', []) if item['name'] == subpath[i]))
      except StopIteration:
        raise NotADirectoryError('drs://' + str(PurePosixPath(host).joinpath(opaque_id, *subpath[:i])))
    return PurePosixPath(host)/info['id'], info

@contextlib.contextmanager
def _open(host, opaque_id, access_method):
  ''' Given access_method, attempt to return its content as a readable file object
  '''
  if access_method.get('access_url'):
    access_url = dict(url=access_method['access_url'])
  elif access_method.get('access_id'):
    access_url = _access(host, opaque_id, access_method['access_id'])
  if access_url['url'].startswith('http://') or access_url['url'].startswith('https://'):
    with urllib.request.urlopen(urllib.request.Request(access_url['url'], headers=access_url.get('headers', {}))) as res:
      yield res
  elif access_url['url'].startswith('ftp://'):
    with urllib.request.urlopen(access_url['url']) as res:
      yield res
  else:
    # fallback to fsspec for other urls, it can support s3/gcloud among others
    try:
      import fsspec
      with fsspec.open(access_url['url'], 'rb') as res:
        yield res
    except KeyboardInterrupt:
      raise
    except:
      traceback.print_exc()
      raise NotImplementedError(access_url['url'])

def info(uri):
  ''' Find information about a drs id
  Usage:
  ```
  print(drs.info('drs://hostname/opaque_id'))
  ```

  :param uri: The drs uri of the form drs://hostname/opaque_id[/name/in/bundle]
  '''
  path = _parse(uri)
  _, info = _flatten(path)
  return info

def ls(uri):
  ''' List objects in a bundle
  Usage:
  ```
  print(drs.ls('drs://hostname/opaque_id'))
  ```

  :param uri: The drs uri of the form drs://hostname/opaque_id[/name/in/bundle]
  '''
  path = _parse(uri)
  flat_path, info = _flatten(path)
  if info.get('contents') is None:
    raise NotADirectoryError('drs://' + str(path))
  return [item['name'] for item in info['contents']]

@contextlib.contextmanager
def open(uri):
  ''' Resolve a DRS URI as a readable file object

  Usage:
  ```
  with drs.open('drs://hostname/opaque_id') as res:
    print(res.read())
  ```

  :param uri: The drs uri of the form drs://hostname/opaque_id[/name/in/bundle]
  :returns: Binary file reader (like open('rb'))
  '''
  path = _parse(uri)
  flat_path, info = _flatten(path)
  host, opaque_id = flat_path.parts
  if info.get('contents') is not None:
    raise IsADirectoryError('drs://' + str(path))
  elif info.get('access_methods') is None:
    info = _info(host, opaque_id, expand=False)
  # find an acceptable access method
  for access_method in info.get('access_methods', []):
    try:
      with _open(host, opaque_id, access_method) as res:
        yield res
      return
    except NotImplementedError:
      pass
  raise NotImplementedError(f"Failed to open object using any of the {info.get('access_methods')}")

def dump(uri, writePath):
  ''' Write the contents of a DRS URI file to an output path

  Usage:
  ```
  uri = 'drs://hostname/opaque_id'
  info = drs.info(uri)
  drs.dump(uri, info['name'])
  ```

  :param uri: The drs uri of the form drs://hostname/opaque_id[/name/in/bundle]
  :param writePath: An output path where the drs object will be written
  '''
  import shutil
  with open(uri) as fr:
    with Path(writePath).open('wb') as fw:
      shutil.copyfileobj(fr, fw)
