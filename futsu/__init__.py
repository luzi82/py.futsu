import lazy_import
import os

name = 'futsu'

DEFAULT_LAZY_ENABLE_SET = set([
    'FUTSU_GCP_ENABLE',
    'FUTSU_AWS_ENABLE',
])

def env_lazy_import(env_var, module_name):
    if env_var in os.environ:
        if os.environ[env_var].lower() in ['0','false']:
            return None
    else:
        if env_var not in DEFAULT_LAZY_ENABLE_SET:
            return None
    return lazy_import.lazy_module(module_name)
