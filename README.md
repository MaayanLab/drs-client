# drs-client

A client for [GA4GH's DRS](https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.2.0/docs/).

## Install
```bash
pip install git+https://github.com/MaayanLab/drs-client.git
```

## Usage
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

### Bundles
Bundles can be introspected with ls, and inner files can be accessed with a relative path like:
```python
import drs_client
print(drs_client.ls('drs://hostname/opaque_id')) # returns files/sub-bundles in the drs bundle
print(drs_client.info('drs://hostname/opaque_id/some/sub/path')) # returns info about the drs object deep in the bundle
```
